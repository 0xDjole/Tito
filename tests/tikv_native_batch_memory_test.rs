mod native;

use native::{create, native_endpoint, native_engine, read, remove};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tikv_client::transaction::ResolveLocksOptions;
use tikv_client::{Key, ProtoLockInfo, Timestamp, TimestampExt};
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::TitoTransaction;
use tito::{
    Guard, TitoEngine, TitoError, TitoIndex, TitoIndexField, TitoIndexValue, TitoModelTrait,
};
use uuid::Uuid;

const CODE: &str = "native-batch-memory-code";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeMemoryRecord {
    id: String,
    scope: String,
    code: String,
}

impl TitoModelTrait for NativeMemoryRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![
            TitoIndex {
                name: "native-batch-memory-by-scope",
                fields: vec![TitoIndexField::Text(self.scope.clone())],
                value: TitoIndexValue::KeysOnly,
                unique: false,
            },
            TitoIndex {
                name: CODE,
                fields: vec![
                    TitoIndexField::Text(self.scope.clone()),
                    TitoIndexField::Text(self.code.clone()),
                ],
                value: TitoIndexValue::Full,
                unique: true,
            },
        ]
    }

    fn table() -> String {
        "native-batch-memory-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn memory_record(scope: &str, name: &str) -> NativeMemoryRecord {
    NativeMemoryRecord {
        id: format!("{scope}-{name}"),
        scope: scope.to_string(),
        code: name.to_string(),
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
    orphan: TiKVTransaction,
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
        "The lock must sit on the row the reader asked for"
    );
    Timestamp::from_version(lock.lock_version)
        .physical
        .checked_add(i64::try_from(lock.lock_ttl).expect("Observed TTL fits PD time"))
        .expect("Observed lock expiry fits PD time")
}

async fn clear_lock(engine: &TiKVBackend, primary: &[u8], secondary: &[u8]) -> Option<Vec<u8>> {
    let now = engine
        .client
        .current_timestamp()
        .await
        .expect("Read actual PD time");
    let mut end = secondary.to_vec();
    end.push(0);
    engine
        .client
        .cleanup_locks(primary.to_vec()..end, &now, ResolveLocksOptions::default())
        .await
        .expect("Roll back the abandoned transaction of this case's lock");
    assert!(locks(engine, primary, secondary).await.is_empty());
    let cleanup = engine
        .begin_transaction()
        .await
        .expect("Begin the cleanup after the lock is gone");
    let left = cleanup.get(secondary).await.unwrap();
    cleanup.delete(primary).await.unwrap();
    cleanup.commit().await.expect("Commit the cleanup");
    left
}

#[tokio::test]
async fn native_a_key_a_batch_read_did_not_find_is_answered_from_memory_while_a_lock_sits_on_it() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = engine.clone().model::<NativeMemoryRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let primary = records.key(&format!("{scope}-0")).into_bytes();
        let missing_id = format!("{scope}-1");
        let never_id = format!("{scope}-2");
        let missing = records.key(&missing_id).into_bytes();
        assert!(primary < missing);

        let orphan = engine.begin_transaction().await.unwrap();
        let reader = engine.begin_transaction().await.unwrap();
        assert_eq!(
            reader
                .batch_get((
                    records.read_maybe(&missing_id, Guard::None),
                    records.read_maybe(&never_id, Guard::None),
                ))
                .await
                .unwrap(),
            (None, None)
        );
        let expiry = leave_lock(&engine, orphan, &primary, &missing).await;
        let started = pd_millis(&engine).await;
        assert!(
            expiry - started >= 1_500,
            "The lock must outlive the reads for this case to prove they sent nothing"
        );

        assert_eq!(reader.get(&missing).await.unwrap(), None);
        assert_eq!(
            reader
                .batch_get((
                    records.read_maybe(&missing_id, Guard::Share),
                    records.read_many(vec![missing_id.clone(), never_id.clone()], Guard::None),
                ))
                .await
                .unwrap(),
            (None, Vec::new())
        );
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
        assert_eq!(clear_lock(&engine, &primary, &missing).await, None);
        assert_eq!(read(&engine, &records, &missing_id).await.unwrap(), None);
    })
    .await
    .expect("The native batch memory case must finish within its deadline");
}

#[tokio::test]
async fn native_a_batch_read_keeps_the_writes_and_locks_of_its_own_transaction() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = engine.clone().model::<NativeMemoryRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let written = memory_record(&scope, "written");
        let deleted = memory_record(&scope, "deleted");
        let locked = memory_record(&scope, "locked");
        let missing = memory_record(&scope, "missing");
        create(&engine, &records, &deleted).await.unwrap();

        let tx = engine.begin_transaction().await.unwrap();
        records.create(&written, &tx).await.unwrap();
        let found = tx
            .batch_get(records.read(&deleted.id, Guard::None))
            .await
            .unwrap();
        records.remove(&found, &tx).await.unwrap();
        tx.inner
            .lock()
            .await
            .lock_keys(vec![Key::from(records.key(&locked.id).into_bytes())])
            .await
            .unwrap();
        assert_eq!(
            tx.batch_get((
                records.read_maybe(&written.id, Guard::None),
                records.read_maybe(&deleted.id, Guard::None),
                records.read_maybe(&locked.id, Guard::None),
                records.read_maybe(&missing.id, Guard::None),
            ))
            .await
            .unwrap(),
            (Some(written.clone()), None, None, None)
        );
        let stored = tx
            .get(records.key(&written.id))
            .await
            .unwrap()
            .expect("The row written in this transaction is read from its own buffer");
        assert_eq!(
            serde_json::from_slice::<NativeMemoryRecord>(&stored).unwrap(),
            written
        );
        assert_eq!(tx.get(records.key(&deleted.id)).await.unwrap(), None);
        assert_eq!(tx.get(records.key(&locked.id)).await.unwrap(), None);
        assert_eq!(tx.get(records.key(&missing.id)).await.unwrap(), None);

        create(&engine, &records, &locked).await.unwrap();
        let kept_tx = tx.clone();
        assert!(
            matches!(tx.commit().await, Err(TitoError::Retryable(_))),
            "The batch read must leave the lock in place, so the later writer conflicts with it"
        );
        let _ = kept_tx.rollback().await;

        let unlocked = memory_record(&scope, "unlocked");
        let kept = memory_record(&scope, "kept");
        let control = engine.begin_transaction().await.unwrap();
        assert_eq!(
            control
                .batch_get(records.read_maybe(&unlocked.id, Guard::None))
                .await
                .unwrap(),
            None
        );
        create(&engine, &records, &unlocked).await.unwrap();
        records.create(&kept, &control).await.unwrap();
        assert_eq!(
            control
                .batch_get((
                    records.read_maybe(&kept.id, Guard::None),
                    records.read_maybe(&unlocked.id, Guard::None),
                ))
                .await
                .unwrap(),
            (Some(kept.clone()), None)
        );
        assert_eq!(control.get(records.key(&unlocked.id)).await.unwrap(), None);
        control
            .commit()
            .await
            .expect("A row that was only read takes no lock, so its writer conflicts with nothing");

        assert_eq!(read(&engine, &records, &written.id).await.unwrap(), None);
        assert_eq!(
            read(&engine, &records, &deleted.id).await.unwrap(),
            Some(deleted.clone())
        );
        assert_eq!(
            read(&engine, &records, &locked.id).await.unwrap(),
            Some(locked.clone())
        );
        assert_eq!(
            read(&engine, &records, &unlocked.id).await.unwrap(),
            Some(unlocked.clone())
        );
        assert_eq!(
            read(&engine, &records, &kept.id).await.unwrap(),
            Some(kept.clone())
        );
        let written_code = records.read_unique_maybe(
            CODE,
            vec![
                TitoIndexField::Text(scope.clone()),
                TitoIndexField::Text(written.code.clone()),
            ],
            Guard::None,
        );
        assert_eq!(
            engine
                .transaction(|tx| async move { tx.batch_get(written_code).await })
                .await
                .unwrap(),
            None
        );
        for id in [&deleted.id, &locked.id, &unlocked.id, &kept.id] {
            remove(&engine, &records, id).await.unwrap();
        }
    })
    .await
    .expect("The native batch memory write case must finish within its deadline");
}

#[tokio::test]
async fn native_a_create_sends_no_read_while_a_lock_sits_on_its_row() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = engine.clone().model::<NativeMemoryRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = memory_record(&scope, "1");
        let primary = records.key(&format!("{scope}-0")).into_bytes();
        let row = records.key(&record.id).into_bytes();
        assert!(primary < row);

        let orphan = engine.begin_transaction().await.unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let expiry = leave_lock(&engine, orphan, &primary, &row).await;
        let started = pd_millis(&engine).await;
        assert!(
            expiry - started >= 1_500,
            "The lock must outlive the create for this case to prove it sent no read"
        );
        records.create(&record, &tx).await.unwrap();
        let finished = pd_millis(&engine).await;
        println!(
            "native create under a lock: lock_remaining_ms={}, create_ms={}",
            expiry - started,
            finished - started
        );
        assert!(
            finished < expiry,
            "The create must finish while the lock on its row is alive, so it read nothing from TiKV"
        );
        tx.rollback().await.unwrap();
        assert_eq!(clear_lock(&engine, &primary, &row).await, None);

        assert_eq!(read(&engine, &records, &record.id).await.unwrap(), None);
        create(&engine, &records, &record).await.unwrap();
        assert_eq!(
            read(&engine, &records, &record.id).await.unwrap(),
            Some(record.clone())
        );
        remove(&engine, &records, &record.id).await.unwrap();
    })
    .await
    .expect("The native create under a lock must finish within its deadline");
}

#[tokio::test]
async fn native_an_update_sends_no_read_while_a_lock_sits_on_its_row() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = engine.clone().model::<NativeMemoryRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = memory_record(&scope, "1");
        let primary = records.key(&format!("{scope}-0")).into_bytes();
        let row = records.key(&record.id).into_bytes();
        assert!(primary < row);
        create(&engine, &records, &record).await.unwrap();

        let orphan = engine.begin_transaction().await.unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let old = tx
            .batch_get(records.read(&record.id, Guard::None))
            .await
            .unwrap();
        assert_eq!(old, record);
        let expiry = leave_lock(&engine, orphan, &primary, &row).await;
        let started = pd_millis(&engine).await;
        assert!(
            expiry - started >= 1_500,
            "The lock must outlive the update for this case to prove it sent no read"
        );
        let changed = NativeMemoryRecord {
            code: "2".into(),
            ..record.clone()
        };
        records.update(&old, &changed, &tx).await.unwrap();
        let finished = pd_millis(&engine).await;
        println!(
            "native update under a lock: lock_remaining_ms={}, update_ms={}",
            expiry - started,
            finished - started
        );
        assert!(
            finished < expiry,
            "The update must finish while the lock on its row is alive, so it read nothing from TiKV"
        );
        tx.rollback().await.unwrap();
        let left = clear_lock(&engine, &primary, &row)
            .await
            .expect("The committed record outlives the abandoned lock");
        assert_eq!(
            serde_json::from_slice::<NativeMemoryRecord>(&left).unwrap(),
            record
        );

        assert_eq!(
            read(&engine, &records, &record.id).await.unwrap(),
            Some(record.clone())
        );
        remove(&engine, &records, &record.id).await.unwrap();
    })
    .await
    .expect("The native update under a lock must finish within its deadline");
}
