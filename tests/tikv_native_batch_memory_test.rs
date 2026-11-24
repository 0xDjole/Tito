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
struct NativeMemoryRecord {
    id: String,
    scope: String,
    code: String,
}

impl TitoModelTrait for NativeMemoryRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "native-memory-by-scope".into(),
            fields: vec![TitoIndexField {
                name: "scope".into(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "native-memory-code".into(),
            fields: vec![
                TitoIndexField {
                    name: "scope".into(),
                    r#type: TitoIndexFieldType::String,
                },
                TitoIndexField {
                    name: "code".into(),
                    r#type: TitoIndexFieldType::String,
                },
            ],
        }]
    }

    fn table() -> String {
        "native-memory-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

async fn pd_millis(engine: &TiKVBackend) -> i64 {
    engine
        .client
        .current_timestamp()
        .await
        .expect("Read actual PD time")
        .physical
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

async fn leave_lock(
    engine: &TiKVBackend,
    orphan: tito::backend::tikv::TiKVTransaction,
    primary: &[u8],
    secondary: &[u8],
) -> i64 {
    let orphan_start = orphan.start_version();
    orphan
        .put(primary, vec![b'p'; 17 * 1024])
        .await
        .expect("Buffer the primary before its secondary");
    orphan
        .put(secondary, b"abandoned secondary")
        .await
        .expect("Buffer the locked key as the secondary of a separate prewrite batch");
    let winner = engine
        .begin_transaction()
        .await
        .expect("Begin the later primary writer");
    winner
        .put(primary, b"native winner")
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
    let observed = locks(engine, primary, secondary).await;
    assert_eq!(observed.len(), 1, "Exactly the secondary lock must be left");
    let lock = &observed[0];
    assert!(
        lock.key == secondary && lock.lock_version == orphan_start && lock.lock_ttl > 0,
        "The lock must sit on the key the reader asked for"
    );
    Timestamp::from_version(lock.lock_version)
        .physical
        .checked_add(i64::try_from(lock.lock_ttl).expect("Observed TTL fits PD time"))
        .expect("Observed lock expiry fits PD time")
}

async fn clear_lock(engine: &TiKVBackend, expiry: i64, primary: &[u8], secondary: &[u8]) {
    while pd_millis(engine).await <= expiry {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let cleanup = engine
        .begin_transaction()
        .await
        .expect("Begin the cleanup after the lock expired");
    assert_eq!(cleanup.get(secondary).await.unwrap(), None);
    cleanup.delete(primary).await.unwrap();
    cleanup.commit().await.expect("Commit the cleanup");
    assert!(locks(engine, primary, secondary).await.is_empty());
}

#[tokio::test]
async fn native_a_key_a_batch_read_did_not_find_is_answered_from_memory_while_a_lock_sits_on_it() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let primary = format!("native-batch-memory:{scope}:0").into_bytes();
        let missing = format!("native-batch-memory:{scope}:1").into_bytes();
        let never = format!("native-batch-memory:{scope}:2").into_bytes();

        let orphan = engine.begin_transaction().await.unwrap();
        let reader = engine.begin_transaction().await.unwrap();
        assert!(reader
            .batch_get(vec![missing.clone(), never.clone()])
            .await
            .unwrap()
            .is_empty());
        let expiry = leave_lock(&engine, orphan, &primary, &missing).await;
        let started = pd_millis(&engine).await;
        assert!(
            expiry - started >= 1_500,
            "The lock must outlive the reads for this case to prove they sent nothing"
        );

        assert_eq!(reader.get(&missing).await.unwrap(), None);
        assert!(reader
            .batch_get(vec![missing.clone(), never.clone()])
            .await
            .unwrap()
            .is_empty());
        let answered = pd_millis(&engine).await;

        let fresh = engine.begin_transaction().await.unwrap();
        let meets_the_lock = fresh
            .inner
            .lock()
            .await
            .batch_get(vec![Key::from(missing.clone())])
            .await
            .map(|pairs| pairs.count());
        let met = pd_millis(&engine).await;
        println!(
            "native batch memory: lock_remaining_ms={}, remembered_reads_ms={}, fresh_read_failed={}",
            expiry - started,
            answered - started,
            meets_the_lock.is_err()
        );
        assert!(
            answered < expiry,
            "The remembered misses must be answered while the lock is alive"
        );
        assert!(
            meets_the_lock.is_err() && met < expiry,
            "A read that is not remembered meets the live lock and gives up"
        );
        fresh.rollback().await.unwrap();
        reader.rollback().await.unwrap();
        clear_lock(&engine, expiry, &primary, &missing).await;
    })
    .await
    .expect("The native batch memory case must finish within its deadline");
}

#[tokio::test]
async fn native_a_batch_read_keeps_the_writes_and_locks_of_its_own_transaction() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let key = |name: &str| format!("native-batch-memory:{scope}:{name}").into_bytes();
        let (written, deleted, locked, missing) = (
            key("written"),
            key("deleted"),
            key("locked"),
            key("missing"),
        );

        let before = engine.begin_transaction().await.unwrap();
        before.put(&deleted, b"before").await.unwrap();
        before.commit().await.unwrap();

        let tx = engine.begin_transaction().await.unwrap();
        tx.put(&written, b"written").await.unwrap();
        tx.delete(&deleted).await.unwrap();
        tx.inner
            .lock()
            .await
            .lock_keys(vec![Key::from(locked.clone())])
            .await
            .unwrap();
        assert_eq!(
            tx.batch_get(vec![
                written.clone(),
                deleted.clone(),
                locked.clone(),
                missing.clone()
            ])
            .await
            .unwrap(),
            vec![(written.clone(), b"written".to_vec())]
        );
        assert_eq!(tx.get(&written).await.unwrap(), Some(b"written".to_vec()));
        assert_eq!(tx.get(&deleted).await.unwrap(), None);
        assert_eq!(tx.get(&locked).await.unwrap(), None);
        assert_eq!(tx.get(&missing).await.unwrap(), None);

        let other = engine.begin_transaction().await.unwrap();
        other.put(&locked, b"other writer").await.unwrap();
        other.commit().await.unwrap();
        assert!(
            matches!(tx.commit().await, Err(TitoError::Retryable(_))),
            "The batch read must leave the lock in place, so the later writer conflicts with it"
        );

        let unlocked = key("unlocked");
        let kept = key("kept");
        let control = engine.begin_transaction().await.unwrap();
        assert!(control
            .batch_get(vec![unlocked.clone()])
            .await
            .unwrap()
            .is_empty());
        let other = engine.begin_transaction().await.unwrap();
        other.put(&unlocked, b"other writer").await.unwrap();
        other.commit().await.unwrap();
        control.put(&kept, b"kept").await.unwrap();
        assert_eq!(
            control
                .batch_get(vec![kept.clone(), unlocked.clone()])
                .await
                .unwrap(),
            vec![(kept.clone(), b"kept".to_vec())]
        );
        assert_eq!(control.get(&unlocked).await.unwrap(), None);
        control
            .commit()
            .await
            .expect("A key that was only read takes no lock, so its writer conflicts with nothing");

        let after = engine.begin_transaction().await.unwrap();
        assert_eq!(after.get(&written).await.unwrap(), None);
        assert_eq!(after.get(&deleted).await.unwrap(), Some(b"before".to_vec()));
        assert_eq!(
            after.get(&locked).await.unwrap(),
            Some(b"other writer".to_vec())
        );
        assert_eq!(after.get(&kept).await.unwrap(), Some(b"kept".to_vec()));
        for name in [&deleted, &locked, &unlocked, &kept] {
            after.delete(name).await.unwrap();
        }
        after.commit().await.unwrap();
    })
    .await
    .expect("The native batch memory write case must finish within its deadline");
}

#[tokio::test]
async fn native_a_save_after_a_batch_read_of_its_save_keys_sends_no_read() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let model = engine
            .clone()
            .model::<NativeMemoryRecord>(TitoModelOptions::default());
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = NativeMemoryRecord {
            id: format!("{scope}-1"),
            scope: scope.clone(),
            code: "first".into(),
        };
        let primary = format!("table:native-memory-records:{scope}-0").into_bytes();
        let row = model.key(&record.id).into_bytes();
        assert!(primary < row);

        let orphan = engine.begin_transaction().await.unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        assert!(tx
            .batch_get(model.save_keys(&record))
            .await
            .unwrap()
            .is_empty());
        let expiry = leave_lock(&engine, orphan, &primary, &row).await;
        let started = pd_millis(&engine).await;
        assert!(
            expiry - started >= 1_500,
            "The lock must outlive the save for this case to prove it sent no read"
        );
        let saved = model
            .set(record.clone())
            .timestamps(false)
            .execute(&tx)
            .await
            .unwrap();
        let finished = pd_millis(&engine).await;
        println!(
            "native save after save_keys: lock_remaining_ms={}, save_ms={}",
            expiry - started,
            finished - started
        );
        assert_eq!(saved, record);
        assert!(
            finished < expiry,
            "The save must finish while the lock on its row is alive, so it read nothing from TiKV"
        );
        tx.rollback().await.unwrap();
        clear_lock(&engine, expiry, &primary, &row).await;
        assert!(matches!(
            model.get_for_share(&record.id, None).await,
            Err(TitoError::NotFound(_))
        ));
        assert_eq!(save(&engine, &model, record.clone()).await.unwrap(), record);
        assert!(remove(&engine, &model, &record.id).await.unwrap());
    })
    .await
    .expect("The native save_keys case must finish within its deadline");
}
