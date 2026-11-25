mod native;

use native::{create, native_endpoint, native_engine, read, remove};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tikv_client::{ProtoLockInfo, Timestamp, TimestampExt};
use tito::backend::tikv::TiKVBackend;
use tito::types::TitoTransaction;
use tito::{
    TitoEngine, TitoError, TitoIndex, TitoIndexField, TitoIndexValue, TitoModel, TitoModelTrait,
};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeOrphanRecord {
    id: String,
    scope: String,
    value: String,
}

impl TitoModelTrait for NativeOrphanRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: "native-orphan-lock-value",
            fields: vec![
                TitoIndexField::Text(self.scope.clone()),
                TitoIndexField::Text(self.value.clone()),
            ],
            value: TitoIndexValue::Full,
            unique: false,
        }]
    }

    fn table() -> String {
        "native-orphan-lock-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

struct Case {
    engine: TiKVBackend,
    records: TitoModel<TiKVBackend, NativeOrphanRecord>,
    record: NativeOrphanRecord,
    primary: Vec<u8>,
    row: Vec<u8>,
}

impl Case {
    async fn new() -> Self {
        let (endpoint, run_id) = native_endpoint();
        let engine = native_engine(endpoint).await;
        let records = engine.clone().model::<NativeOrphanRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = NativeOrphanRecord {
            id: format!("{scope}-1"),
            scope: scope.clone(),
            value: "after the lock".into(),
        };
        let primary = records.key(&format!("{scope}-0")).into_bytes();
        let row = records.key(&record.id).into_bytes();
        assert!(primary < row);
        Case {
            engine,
            records,
            record,
            primary,
            row,
        }
    }

    fn end(&self) -> Vec<u8> {
        let mut end = self.row.clone();
        end.push(0);
        end
    }

    async fn pd_millis(&self) -> i64 {
        self.engine
            .client
            .current_timestamp()
            .await
            .expect("Read actual PD time")
            .physical
    }

    async fn locks(&self) -> Vec<ProtoLockInfo> {
        let now = self
            .engine
            .client
            .current_timestamp()
            .await
            .expect("Read actual PD time");
        self.engine
            .client
            .scan_locks(&now, self.primary.clone()..self.end(), 16)
            .await
            .expect("Inspect only the locks of this case's keys")
    }

    async fn leave_lock(&self) -> i64 {
        let orphan = self
            .engine
            .begin_transaction()
            .await
            .expect("Begin the save that leaves its lock");
        let orphan_start = orphan.start_version();
        orphan
            .put(&self.primary, vec![b'p'; 17 * 1024])
            .await
            .expect("Buffer the primary in a prewrite batch of its own");
        orphan
            .put(&self.row, b"abandoned row")
            .await
            .expect("Buffer the record row in another prewrite batch");
        let winner = self
            .engine
            .begin_transaction()
            .await
            .expect("Begin the later writer of the primary");
        winner
            .put(&self.primary, b"native winner")
            .await
            .expect("Write only the primary");
        winner
            .commit()
            .await
            .expect("Commit the primary before the older prewrite");
        assert!(
            matches!(orphan.commit().await, Err(TitoError::Retryable(_))),
            "The older prewrite of the primary must conflict"
        );
        let observed = self.locks().await;
        assert_eq!(
            observed.len(),
            1,
            "Exactly the record row's lock must be left"
        );
        let lock = &observed[0];
        assert!(
            lock.key == self.row
                && lock.primary_lock == self.primary
                && lock.lock_version == orphan_start
                && lock.lock_ttl > 0,
            "The lock must sit on the record row and name the primary that was never written"
        );
        let expiry = Timestamp::from_version(lock.lock_version)
            .physical
            .checked_add(i64::try_from(lock.lock_ttl).expect("Observed TTL fits PD time"))
            .expect("Observed lock expiry fits PD time");
        assert!(
            self.pd_millis().await < expiry,
            "The save under test must begin while the lock is alive"
        );
        expiry
    }

    async fn passed(&self, expiry: i64) {
        assert!(
            self.pd_millis().await >= expiry,
            "The save can pass the lock only after its lifetime ended"
        );
        assert!(
            self.locks().await.is_empty(),
            "No lock of the abandoned save is left"
        );
    }

    async fn stored(&self, key: &[u8]) -> Option<Vec<u8>> {
        let tx = self
            .engine
            .begin_transaction()
            .await
            .expect("Begin a fresh read");
        let value = tx.get(key).await.expect("Read one key of this case");
        tx.commit().await.expect("Complete the fresh read");
        value
    }

    async fn finish(&self, created: bool) {
        assert_eq!(
            self.stored(&self.primary).await,
            Some(b"native winner".to_vec()),
            "The committed primary is untouched"
        );
        if created {
            remove(&self.engine, &self.records, &self.record.id)
                .await
                .expect("Remove this case's record");
        }
        let tx = self
            .engine
            .begin_transaction()
            .await
            .expect("Begin the cleanup of the primary");
        tx.delete(&self.primary)
            .await
            .expect("Delete this case's primary");
        tx.commit().await.expect("Commit the cleanup");
        assert_eq!(self.stored(&self.primary).await, None);
        assert_eq!(self.stored(&self.row).await, None);
    }
}

#[tokio::test]
async fn native_a_batch_read_that_meets_a_lock_whose_primary_was_never_written_passes_it_once_its_lifetime_ends(
) {
    tokio::time::timeout(Duration::from_secs(60), async move {
        let case = Case::new().await;
        let expiry = case.leave_lock().await;
        let found = read(&case.engine, &case.records, &case.record.id)
            .await
            .expect("The batch read waits for the lock and then clears it");
        case.passed(expiry).await;
        assert_eq!(found, None, "The abandoned row was never committed");
        case.finish(false).await;
    })
    .await
    .expect("The native case must finish within its deadline");
}

#[tokio::test]
async fn native_a_single_read_that_meets_a_lock_whose_primary_was_never_written_passes_it_once_its_lifetime_ends(
) {
    tokio::time::timeout(Duration::from_secs(60), async move {
        let case = Case::new().await;
        let expiry = case.leave_lock().await;
        let row = case.row.clone();
        let found = case
            .engine
            .transaction(move |tx| {
                let row = row.clone();
                async move { tx.get(&row).await }
            })
            .await
            .expect("The read waits for the lock and then clears it");
        case.passed(expiry).await;
        assert_eq!(found, None, "The abandoned row was never committed");
        case.finish(false).await;
    })
    .await
    .expect("The native case must finish within its deadline");
}

#[tokio::test]
async fn native_a_scan_that_meets_a_lock_whose_primary_was_never_written_passes_it_once_its_lifetime_ends(
) {
    tokio::time::timeout(Duration::from_secs(60), async move {
        let case = Case::new().await;
        let expiry = case.leave_lock().await;
        let (start, end) = (case.primary.clone(), case.end());
        let rows = case
            .engine
            .transaction(move |tx| {
                let (start, end) = (start.clone(), end.clone());
                async move { tx.scan(start..end, 10).await }
            })
            .await
            .expect("The scan waits for the lock and then clears it");
        case.passed(expiry).await;
        assert_eq!(
            rows,
            vec![(case.primary.clone(), b"native winner".to_vec())],
            "The scan finds the committed primary and nothing of the abandoned save"
        );
        case.finish(false).await;
    })
    .await
    .expect("The native case must finish within its deadline");
}

#[tokio::test]
async fn native_a_create_that_meets_a_lock_whose_primary_was_never_written_passes_it_once_its_lifetime_ends(
) {
    tokio::time::timeout(Duration::from_secs(60), async move {
        let case = Case::new().await;
        let expiry = case.leave_lock().await;
        create(&case.engine, &case.records, &case.record)
            .await
            .expect("The create waits for the lock and then clears it");
        case.passed(expiry).await;
        assert_eq!(
            read(&case.engine, &case.records, &case.record.id)
                .await
                .expect("Read the created record"),
            Some(case.record.clone())
        );
        case.finish(true).await;
    })
    .await
    .expect("The native case must finish within its deadline");
}
