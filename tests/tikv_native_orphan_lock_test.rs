use log::{Level, LevelFilter, Log, Metadata, Record};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tikv_client::{ProtoLockInfo, Timestamp, TimestampExt};
use tito::backend::tikv::{TiKV, TiKVBackend};
use tito::types::TitoTransaction;
use tito::{TitoEngine, TitoError};
use uuid::Uuid;

struct ExpectedOrphan {
    key: Vec<u8>,
    primary: Vec<u8>,
    start_version: u64,
    caller_start_version: u64,
}

struct ExpiredOrphanWitness {
    expected: Mutex<Option<ExpectedOrphan>>,
    matched: AtomicUsize,
}

static WITNESS: ExpiredOrphanWitness = ExpiredOrphanWitness {
    expected: Mutex::new(None),
    matched: AtomicUsize::new(0),
};

fn native_key_field(text: &str, field: &str, key: &[u8]) -> bool {
    let ascii = std::str::from_utf8(key).expect("Proof keys are bounded ASCII identities");
    text.contains(&format!("{field}: {key:?}")) || text.contains(&format!("{field}: b\"{ascii}\""))
}

impl Log for ExpiredOrphanWitness {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() == Level::Warn && metadata.target() == "tikv_client::transaction::lock"
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let expected = self
            .expected
            .lock()
            .expect("Native witness must remain usable");
        let Some(expected) = expected.as_ref() else {
            return;
        };
        let message = record.args().to_string();
        if message.len() <= 4096
            && message.starts_with("lock txn not found, lock has expired, lock ")
            && native_key_field(&message, "key", &expected.key)
            && native_key_field(&message, "primary_lock", &expected.primary)
            && message.contains(&format!("lock_version: {},", expected.start_version))
            && message.contains(&format!(
                ", caller_start_ts {}, current_ts ",
                expected.caller_start_version
            ))
        {
            self.matched.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn flush(&self) {}
}

fn native_endpoint() -> (String, String) {
    let endpoint = std::env::var("TITO_NATIVE_PD_URI").expect(
        "TITO_NATIVE_PD_URI must explicitly name the PD endpoint owned by cargo arky test serve",
    );
    let address: SocketAddr = endpoint
        .parse()
        .expect("TITO_NATIVE_PD_URI must be an explicit loopback host:port");
    assert!(
        address.ip().is_loopback() && address.port() != 0,
        "The native proof requires its declared disposable loopback endpoint"
    );
    let run_id = std::env::var("TITO_NATIVE_RUN_ID")
        .expect("TITO_NATIVE_RUN_ID must be the existing disposable test serve run ID");
    assert!(
        run_id.starts_with("run-")
            && run_id.len() > 4
            && run_id.len() <= 64
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "The native proof requires an explicit bounded disposable run identity"
    );
    (endpoint, run_id)
}

async fn locks(backend: &TiKVBackend, primary: &[u8], secondary: &[u8]) -> Vec<ProtoLockInfo> {
    let now = backend
        .client
        .current_timestamp()
        .await
        .expect("Read actual PD time");
    let mut end = secondary.to_vec();
    end.push(0);
    backend
        .client
        .scan_locks(&now, primary.to_vec()..end, 2)
        .await
        .expect("Inspect only the native locks in this proof's UUID scope")
}

async fn values(
    backend: &TiKVBackend,
    primary: &[u8],
    secondary: &[u8],
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let tx = backend
        .begin_transaction()
        .await
        .expect("Begin a fresh normal Tito read");
    let primary = tx
        .get(primary)
        .await
        .expect("Read the proof's exact primary");
    let secondary = tx
        .get(secondary)
        .await
        .expect("Read the proof's exact secondary");
    tx.commit().await.expect("Complete the normal Tito read");
    (primary, secondary)
}

#[tokio::test]
async fn expired_secondary_without_a_primary_heals_through_normal_tito_reads() {
    let (endpoint, run_id) = native_endpoint();
    log::set_logger(&WITNESS).expect("The native witness must not replace an initialized logger");
    log::set_max_level(LevelFilter::Warn);
    tokio::time::timeout(Duration::from_secs(30), async move {
        let backend = TiKV::connect(vec![endpoint])
            .await
            .expect("Connect to the declared test-owned native PD and TiKV");
        let scope = format!("tito:native:orphan:{run_id}:{}:", Uuid::new_v4());
        let primary = format!("{scope}primary").into_bytes();
        let secondary = format!("{scope}secondary").into_bytes();
        assert!(primary < secondary);
        assert_eq!(values(&backend, &primary, &secondary).await, (None, None));

        let orphan = backend.begin_transaction().await.expect("Begin the normal two-key Tito transaction");
        let orphan_start = orphan.start_version();
        orphan.put(&primary, vec![b'p'; 17 * 1024]).await.expect("Buffer the real primary before its secondary");
        orphan.put(&secondary, b"abandoned secondary").await.expect("Buffer the real secondary in a separate native prewrite batch");

        let winner = backend.begin_transaction().await.expect("Begin the actual later primary writer");
        let winner_start = winner.start_version();
        assert!(winner_start > orphan_start);
        winner.put(&primary, b"native winner").await.expect("Write only the proof's actual primary");
        winner.commit().await.expect("Commit the real primary winner before the older prewrite");

        let error = orphan.commit().await.expect_err("The older actual primary prewrite must conflict");
        let TitoError::Retryable(native_error) = error else {
            panic!("The actual primary must produce a definite retryable conflict, not an unknown outcome")
        };
        assert!(
            native_error.matches("conflict: Some(WriteConflict").count() == 1
                && native_error.contains(&format!("start_ts: {orphan_start},"))
                && native_error.contains(&format!("conflict_ts: {winner_start},"))
                && native_key_field(&native_error, "key", &primary)
                && native_key_field(&native_error, "primary", &primary),
            "The actual native WriteConflict must bind this transaction and the exact winning primary"
        );

        let observed = locks(&backend, &primary, &secondary).await;
        assert_eq!(observed.len(), 1, "The failed real two-batch prewrite must leave exactly its native secondary lock");
        let lock = &observed[0];
        assert!(
            lock.key == secondary
                && lock.primary_lock == primary
                && lock.lock_version == orphan_start
                && lock.lock_ttl > 0,
            "The native residue must be the exact secondary referencing its absent transaction primary"
        );
        let before = backend.begin_transaction().await.expect("Read the committed winner without touching the orphaned secondary");
        assert_eq!(before.get(&primary).await.expect("Read the winning primary"), Some(b"native winner".to_vec()));
        before.commit().await.expect("Complete the winning-primary read");

        let expiry = Timestamp::from_version(lock.lock_version)
            .physical
            .checked_add(i64::try_from(lock.lock_ttl).expect("Observed native TTL must fit physical PD time"))
            .expect("Observed native lock expiry must fit physical PD time");
        loop {
            let now = backend.client.current_timestamp().await.expect("Read the actual PD timestamp for the observed native TTL");
            if now.physical >= expiry {
                break;
            }
            let remaining = u64::try_from(expiry - now.physical).expect("Remaining native TTL is positive");
            tokio::time::sleep(Duration::from_millis(remaining)).await;
        }

        let reader = backend.begin_transaction().await.expect("Begin the fresh normal Tito read that must resolve its native orphan");
        let reader_start = reader.start_version();
        assert!(reader_start > winner_start);
        *WITNESS.expected.lock().unwrap() = Some(ExpectedOrphan {
            key: lock.key.clone(),
            primary: lock.primary_lock.clone(),
            start_version: lock.lock_version,
            caller_start_version: reader_start,
        });
        let healed = reader.get(&secondary).await;
        let branch_count = WITNESS.matched.load(Ordering::SeqCst);
        println!("native Tito orphan branch: definite_primary_conflicts=1, observed_secondary_locks=1, observed_lock_ttl_ms={}, expired_missing_primary_branches={branch_count}, normal_read_succeeded={}", lock.lock_ttl, healed.is_ok());
        assert!(healed.is_ok(), "The fresh normal Tito read must heal the actual expired missing-primary lock");
        assert_eq!(healed.unwrap(), None);
        assert!(branch_count > 0, "The exact native missing-primary expiry branch must be observed for this real reader and lock");
        assert_eq!(reader.get(&primary).await.expect("Verify the normal read preserved the actual primary winner"), Some(b"native winner".to_vec()));
        reader.commit().await.expect("Complete the normal read after native lock resolution");
        assert!(locks(&backend, &primary, &secondary).await.is_empty(), "The real orphaned secondary lock must be gone after the normal read");

        let recovered = backend.begin_transaction().await.expect("Begin a fresh normal writer on the healed exact secondary");
        recovered.put(&secondary, b"native recovered").await.expect("Write the healed secondary through normal Tito");
        recovered.commit().await.expect("Commit the real recovered secondary without any lock repair helper");
        assert_eq!(values(&backend, &primary, &secondary).await, (Some(b"native winner".to_vec()), Some(b"native recovered".to_vec())));
        let cleanup = backend.begin_transaction().await.expect("Begin the normal exact two-key cleanup");
        cleanup.delete(&primary).await.expect("Delete only this proof's primary");
        cleanup.delete(&secondary).await.expect("Delete only this proof's secondary");
        cleanup.commit().await.expect("Commit the normal exact two-key cleanup");
        assert_eq!(values(&backend, &primary, &secondary).await, (None, None));
        println!("native Tito orphan recovery: remaining_secondary_locks=0, recovered_keys=1, preserved_winner_keys=1, cleaned_keys=2");
    })
    .await
    .expect("The real native orphan proof must finish within its bounded deadline");
}
