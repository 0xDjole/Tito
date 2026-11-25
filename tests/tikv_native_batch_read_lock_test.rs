mod native;

use native::{create, native_endpoint, native_engine, read, remove};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tikv_client::{Key, ProtoLockInfo, Timestamp, TimestampExt};
use tito::backend::tikv::TiKVBackend;
use tito::types::TitoTransaction;
use tito::{
    Guard, TitoEngine, TitoError, TitoIndex, TitoIndexField, TitoIndexValue, TitoModelTrait,
};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeLockedRecord {
    id: String,
    scope: String,
    value: String,
}

impl TitoModelTrait for NativeLockedRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: "native-batch-read-lock-value",
            fields: vec![
                TitoIndexField::Text(self.scope.clone()),
                TitoIndexField::Text(self.value.clone()),
            ],
            value: TitoIndexValue::Full,
            unique: true,
        }]
    }

    fn table() -> String {
        "native-batch-read-lock-records".into()
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
async fn native_a_note_whose_key_holds_a_live_lock_waits_for_it_like_a_single_read() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = engine.clone().model::<NativeLockedRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = NativeLockedRecord {
            id: format!("{scope}-1"),
            scope: scope.clone(),
            value: "locked".into(),
        };
        let neighbor = NativeLockedRecord {
            id: format!("{scope}-2"),
            scope: scope.clone(),
            value: "neighbor".into(),
        };
        let primary = records.key(&format!("{scope}-0")).into_bytes();
        let secondary = records.key(&record.id).into_bytes();
        assert!(primary < secondary);
        create(&engine, &records, &neighbor).await.unwrap();

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
            .expect("Buffer the record row as the secondary of a separate prewrite batch");
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
            "The failed prewrite must leave exactly the lock on the record row"
        );
        let lock = &observed[0];
        assert!(
            lock.key == secondary
                && lock.primary_lock == primary
                && lock.lock_version == orphan_start
                && lock.lock_ttl > 0,
            "The lock must sit on the record row and name the absent transaction primary"
        );
        let expiry = Timestamp::from_version(lock.lock_version)
            .physical
            .checked_add(i64::try_from(lock.lock_ttl).expect("Observed TTL fits PD time"))
            .expect("Observed lock expiry fits PD time");

        let tx = engine
            .begin_transaction()
            .await
            .expect("Begin the reading transaction after the lock");
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
            let found = tx
                .batch_get((
                    records.read_maybe(&record.id, Guard::None),
                    records.read(&neighbor.id, Guard::None),
                ))
                .await;
            (found, pd_millis(&engine).await)
        };
        let (client_only, (found, finished)) = tokio::join!(client_only, tito);
        println!(
            "native batch read lock wait: lock_ttl_ms={}, remaining_at_start_ms={}, client_only_failed={}, note_succeeded={}, finished_after_expiry_ms={}",
            lock.lock_ttl,
            expiry - started,
            client_only.is_err(),
            found.is_ok(),
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
            found.expect("The note must wait for the lock inside its own transaction"),
            (None, neighbor.clone())
        );
        assert!(
            finished >= expiry,
            "The note can only read the record row after the lock expired"
        );
        assert!(locks(&engine, &primary, &secondary).await.is_empty());
        records
            .create(&record, &tx)
            .await
            .expect("Create the record the note found missing");
        tx.commit()
            .await
            .expect("Commit the create made after the lock was resolved");

        assert_eq!(
            read(&engine, &records, &record.id).await.unwrap(),
            Some(record.clone())
        );
        let renamed = NativeLockedRecord {
            value: "unlocked".into(),
            ..record.clone()
        };
        let note = records.read(&record.id, Guard::None);
        engine
            .transaction(|tx| {
                let records = records.clone();
                let note = note.clone();
                let renamed = renamed.clone();
                async move {
                    let old = tx.batch_get(note).await?;
                    records.update(&old, &renamed, &tx).await
                }
            })
            .await
            .unwrap();
        assert_eq!(
            read(&engine, &records, &record.id).await.unwrap(),
            Some(renamed)
        );
        remove(&engine, &records, &record.id).await.unwrap();
        remove(&engine, &records, &neighbor.id).await.unwrap();
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
