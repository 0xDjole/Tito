mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tikv_client::{Key, ProtoLockInfo, Timestamp, TimestampExt};
use tito::backend::tikv::TiKVBackend;
use tito::types::{TitoIndexConfig, TitoIndexField, TitoIndexFieldType, TitoTransaction};
use tito::{TitoEngine, TitoError, TitoModelOptions, TitoModelTrait};
use uuid::Uuid;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeLockedRecord {
    id: String,
    scope: String,
    value: String,
}

impl TitoModelTrait for NativeLockedRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "native-batch-lock-value".into(),
            fields: vec![
                TitoIndexField {
                    name: "scope".into(),
                    r#type: TitoIndexFieldType::String,
                },
                TitoIndexField {
                    name: "value".into(),
                    r#type: TitoIndexFieldType::String,
                },
            ],
        }]
    }

    fn table() -> String {
        "native-batch-lock-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

async fn locks(engine: &TiKVBackend, primary: &[u8], secondary: &[u8]) -> Vec<ProtoLockInfo> {
    let now = engine
        .client
        .current_timestamp()
        .await
        .expect("Read actual PD time");
    let mut end = secondary.to_vec();
    end.push(0);
    engine
        .client
        .scan_locks(&now, primary.to_vec()..end, 2)
        .await
        .expect("Inspect only the native locks in this case's UUID scope")
}

async fn pd_millis(engine: &TiKVBackend) -> i64 {
    engine
        .client
        .current_timestamp()
        .await
        .expect("Read actual PD time")
        .physical
}

#[tokio::test]
async fn native_a_save_whose_batch_read_meets_a_live_lock_waits_for_it_like_a_single_read() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let model = engine
            .clone()
            .model::<NativeLockedRecord>(TitoModelOptions::default());
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = NativeLockedRecord {
            id: format!("{scope}-1"),
            scope: scope.clone(),
            value: "locked".into(),
        };
        let primary = format!("table:native-batch-lock-records:{scope}-0").into_bytes();
        let secondary = format!("table:native-batch-lock-records:{}", record.id).into_bytes();
        assert!(primary < secondary);

        let orphan = engine
            .begin_transaction()
            .await
            .expect("Begin the two-key transaction that leaves the lock");
        let orphan_start = orphan.start_version();
        orphan
            .put(&primary, vec![b'p'; 17 * 1024])
            .await
            .expect("Buffer the primary before its secondary");
        orphan
            .put(&secondary, b"abandoned secondary")
            .await
            .expect("Buffer the record key as the secondary of a separate prewrite batch");
        let winner = engine
            .begin_transaction()
            .await
            .expect("Begin the later primary writer");
        winner
            .put(&primary, b"native winner")
            .await
            .expect("Write only the primary");
        winner
            .commit()
            .await
            .expect("Commit the primary before the older prewrite");
        assert!(
            matches!(orphan.commit().await, Err(TitoError::Retryable(_))),
            "The older primary prewrite must conflict"
        );

        let observed = locks(&engine, &primary, &secondary).await;
        assert_eq!(
            observed.len(),
            1,
            "The failed prewrite must leave exactly the lock on the record key"
        );
        let lock = &observed[0];
        assert!(
            lock.key == secondary
                && lock.primary_lock == primary
                && lock.lock_version == orphan_start
                && lock.lock_ttl > 0,
            "The lock must sit on the record key and name the absent transaction primary"
        );
        let expiry = Timestamp::from_version(lock.lock_version)
            .physical
            .checked_add(i64::try_from(lock.lock_ttl).expect("Observed TTL fits PD time"))
            .expect("Observed lock expiry fits PD time");

        let tx = engine
            .begin_transaction()
            .await
            .expect("Begin the saving transaction after the lock");
        let reader = engine
            .begin_transaction()
            .await
            .expect("Begin the client-only reader after the lock");
        let started = pd_millis(&engine).await;
        assert!(
            expiry - started >= 2_000,
            "The lock must outlive one client read attempt of about 1.5 s for this case to prove a wait"
        );
        let client_only = async {
            reader
                .inner
                .lock()
                .await
                .batch_get(vec![Key::from(secondary.clone())])
                .await
                .map(|pairs| pairs.count())
        };
        let tito = async {
            let saved = model
                .set(record.clone())
                .timestamps(false)
                .execute(&tx)
                .await;
            (saved, pd_millis(&engine).await)
        };
        let (client_only, (saved, finished)) = tokio::join!(client_only, tito);
        println!(
            "native batch read lock wait: lock_ttl_ms={}, remaining_at_start_ms={}, client_only_failed={}, save_succeeded={}, finished_after_expiry_ms={}",
            lock.lock_ttl,
            expiry - started,
            client_only.is_err(),
            saved.is_ok(),
            finished - expiry
        );
        assert!(
            client_only.is_err(),
            "A single client batch read must give up while the lock is alive"
        );
        reader
            .rollback()
            .await
            .expect("Close the client-only reader");
        assert_eq!(
            saved.expect("The save must wait for the lock inside its own transaction"),
            record
        );
        assert!(
            finished >= expiry,
            "The save can only read the record key after the lock expired"
        );
        assert!(locks(&engine, &primary, &secondary).await.is_empty());
        tx.commit()
            .await
            .expect("Commit the save made after the lock was resolved");

        assert_eq!(
            model.get_for_share(&record.id, None).await.unwrap(),
            record
        );
        let renamed = NativeLockedRecord {
            value: "unlocked".into(),
            ..record.clone()
        };
        assert_eq!(save(&engine, &model, renamed.clone()).await.unwrap(), renamed);
        assert!(remove(&engine, &model, &record.id).await.unwrap());
        let cleanup = engine
            .begin_transaction()
            .await
            .expect("Begin the primary cleanup");
        cleanup
            .delete(&primary)
            .await
            .expect("Delete only this case's primary");
        cleanup.commit().await.expect("Commit the primary cleanup");
    })
    .await
    .expect("The native lock wait case must finish within its deadline");
}
