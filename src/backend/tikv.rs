use crate::error::TitoError;
use crate::fence::{guard_keys, note_guard, note_write, Guards};
use crate::index::RememberedEntry;
use crate::read::{Noted, Request, Rows};
use crate::types::{
    validate_delete_range, DBUuid, Guard, Sealed, TitoEngine, TitoKvPair, TitoTransaction,
    TitoValue,
};
use crate::utils::key_after_bytes;
use async_trait::async_trait;
use futures::lock::Mutex;
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tikv_client::transaction::ResolveLocksOptions;
use tikv_client::{
    ProtoLockInfo, Snapshot, Timestamp, TimestampExt, Transaction, TransactionClient,
    TransactionOptions,
};
use tokio::time::sleep;

pub(crate) const MAX_TRANSACTION_RETRIES: u32 = 10;
const MAX_READ_LOCK_RESOLUTION_ATTEMPTS: u32 = 8;
const READ_LOCK_RESOLUTION_BACKOFF: Duration = Duration::from_millis(25);
const TIMESTAMP_PATIENCE: Duration = Duration::from_secs(10);
const TIMESTAMP_CHANNEL_CLOSED: &str = "TimestampRequest channel is closed";
const SHORTEST_LOCK_LIFETIME_MS: i64 = 3_000;
const LOCK_SCAN_LIMIT: u32 = 1_024;
const TIKV_GRPC_MAX_DECODING_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
pub const SCAN_RPC_BYTES: usize = 4 * 1024 * 1024;
pub const SCAN_RPC_FIRST_ROWS: u32 = 16;
pub const SCAN_RPC_MAX_ROWS: u32 = 4_096;
const GRPC_RESOURCE_EXHAUSTED: i32 = 8;
const GRPC_OUT_OF_RANGE: i32 = 11;

fn exceeds_message_limit(error: &tikv_client::Error) -> bool {
    match error {
        tikv_client::Error::GrpcAPI(status) => matches!(
            i32::from(status.code()),
            GRPC_RESOURCE_EXHAUSTED | GRPC_OUT_OF_RANGE
        ),
        tikv_client::Error::MultipleKeyErrors(errors)
        | tikv_client::Error::ExtractedErrors(errors) => errors.iter().any(exceeds_message_limit),
        _ => false,
    }
}

fn scan_rows_for(largest_row_bytes: usize) -> u32 {
    (SCAN_RPC_BYTES / largest_row_bytes.max(1)).clamp(1, SCAN_RPC_MAX_ROWS as usize) as u32
}

fn contains_undetermined_error(error: &tikv_client::Error) -> bool {
    match error {
        tikv_client::Error::UndeterminedError(_) => true,
        tikv_client::Error::MultipleKeyErrors(errors)
        | tikv_client::Error::ExtractedErrors(errors) => {
            errors.iter().any(contains_undetermined_error)
        }
        _ => false,
    }
}

fn contains_grpc_error(error: &tikv_client::Error) -> bool {
    match error {
        tikv_client::Error::Grpc(_) | tikv_client::Error::GrpcAPI(_) => true,
        tikv_client::Error::MultipleKeyErrors(errors)
        | tikv_client::Error::ExtractedErrors(errors) => errors.iter().any(contains_grpc_error),
        _ => false,
    }
}

fn is_retryable_tikv_error(error: &tikv_client::Error) -> bool {
    match error {
        tikv_client::Error::RegionError(_)
        | tikv_client::Error::RegionForKeyNotFound { .. }
        | tikv_client::Error::RegionForRangeNotFound { .. }
        | tikv_client::Error::RegionNotFoundInResponse { .. }
        | tikv_client::Error::LeaderNotFound { .. }
        | tikv_client::Error::ResolveLockError(_)
        | tikv_client::Error::TxnNotFound(_)
        | tikv_client::Error::NoCurrentRegions
        | tikv_client::Error::EntryNotFoundInRegionCache
        | tikv_client::Error::PessimisticLockError { .. }
        | tikv_client::Error::Channel(_) => true,
        tikv_client::Error::InternalError { message } => message == TIMESTAMP_CHANNEL_CLOSED,
        tikv_client::Error::KeyError(key_error) => {
            key_error.locked.is_some()
                || key_error.conflict.is_some()
                || key_error.txn_not_found.is_some()
                || !key_error.retryable.is_empty()
        }
        tikv_client::Error::MultipleKeyErrors(errors)
        | tikv_client::Error::ExtractedErrors(errors) => {
            !errors.is_empty() && errors.iter().all(is_retryable_tikv_error)
        }
        _ => false,
    }
}

fn already_exist_key(error: &tikv_client::Error) -> Option<Vec<u8>> {
    match error {
        tikv_client::Error::KeyError(key_error) => key_error
            .already_exist
            .as_ref()
            .map(|exist| exist.key.clone()),
        tikv_client::Error::MultipleKeyErrors(errors)
        | tikv_client::Error::ExtractedErrors(errors) => errors.iter().find_map(already_exist_key),
        _ => None,
    }
}

fn missing_lock_owners(error: &tikv_client::Error) -> Vec<u64> {
    match error {
        tikv_client::Error::TxnNotFound(missing) => vec![missing.start_ts],
        tikv_client::Error::KeyError(key_error) => key_error
            .txn_not_found
            .iter()
            .map(|missing| missing.start_ts)
            .collect(),
        tikv_client::Error::MultipleKeyErrors(errors)
        | tikv_client::Error::ExtractedErrors(errors) => {
            errors.iter().flat_map(missing_lock_owners).collect()
        }
        _ => Vec::new(),
    }
}

fn lock_expired(lock: &ProtoLockInfo, now: &Timestamp) -> bool {
    let lifetime = i64::try_from(lock.lock_ttl).unwrap_or(i64::MAX);
    Timestamp::from_version(lock.lock_version)
        .physical
        .saturating_add(lifetime)
        <= now.physical
}

fn timestamp_wait(attempt: u32) -> Duration {
    Duration::from_millis((50u64 << attempt.min(6)).min(2_000))
}

fn classify_tikv_error(
    e: tikv_client::Error,
    context: &str,
    retryable_flag: &AtomicBool,
) -> TitoError {
    let msg = format!("{}: {}", context, e);
    if contains_undetermined_error(&e) {
        return TitoError::CommitOutcomeUnknown(msg);
    }

    if is_retryable_tikv_error(&e) {
        retryable_flag.store(true, Ordering::Relaxed);
        TitoError::Retryable(msg)
    } else {
        TitoError::TransactionFailed(msg)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitFailureAction {
    Retry,
    Return,
}

fn commit_failure_action(error: &TitoError, retries: u32) -> CommitFailureAction {
    if error.is_retryable() && retries < MAX_TRANSACTION_RETRIES {
        CommitFailureAction::Retry
    } else {
        CommitFailureAction::Return
    }
}

fn gc_safepoint(mut current: Timestamp, retention: Duration) -> Timestamp {
    let retention_millis = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
    current.physical = current.physical.saturating_sub(retention_millis).max(0);
    current.logical = 0;
    current
}

#[derive(Clone)]
pub struct TiKVBackend {
    pub client: Arc<TransactionClient>,
    pub active_transactions: Arc<Mutex<HashMap<String, TiKVTransaction>>>,
}

struct TransactionRegistration {
    backend: TiKVBackend,
    transaction: Option<TiKVTransaction>,
}

impl TransactionRegistration {
    async fn finish(&mut self) {
        if let Some(transaction) = &self.transaction {
            self.backend
                .active_transactions
                .lock()
                .await
                .remove(&transaction.id);
        }
        self.transaction = None;
    }
}

impl Drop for TransactionRegistration {
    fn drop(&mut self) {
        let Some(transaction) = self.transaction.take() else {
            return;
        };
        let backend = self.backend.clone();
        tokio::spawn(async move {
            backend
                .active_transactions
                .lock()
                .await
                .remove(&transaction.id);
            if !transaction.commit_started.load(Ordering::Acquire) {
                let _ = transaction.rollback().await;
            }
        });
    }
}

#[async_trait]
impl TitoEngine for TiKVBackend {
    type Transaction = TiKVTransaction;

    async fn begin_transaction(&self) -> Result<Self::Transaction, TitoError> {
        let first_try = Instant::now();
        let mut attempt = 0;
        let tx = loop {
            match self.client.begin_optimistic().await {
                Ok(tx) => break tx,
                Err(error) => {
                    attempt += 1;
                    let wait = timestamp_wait(attempt);
                    if first_try.elapsed() + wait > TIMESTAMP_PATIENCE {
                        let no_flag = AtomicBool::new(false);
                        return Err(classify_tikv_error(
                            error,
                            "Failed to begin transaction",
                            &no_flag,
                        ));
                    }
                    sleep(wait).await;
                }
            }
        };
        let start_version = tx.start_timestamp().version();
        let snapshot = self
            .client
            .snapshot(tx.start_timestamp(), TransactionOptions::new_optimistic());
        Ok(TiKVTransaction {
            id: DBUuid::new_v4().to_string(),
            inner: Arc::new(tokio::sync::Mutex::new(tx)),
            snapshot: Arc::new(tokio::sync::Mutex::new(snapshot)),
            client: self.client.clone(),
            had_retryable_error: Arc::new(AtomicBool::new(false)),
            commit_started: Arc::new(AtomicBool::new(false)),
            wrote: Arc::new(AtomicBool::new(false)),
            guards: Arc::new(tokio::sync::Mutex::new(Guards::default())),
            records: Arc::new(std::sync::Mutex::new(HashMap::new())),
            written: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
            missing: Arc::new(std::sync::Mutex::new(HashSet::new())),
            inserts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            refused: Arc::new(std::sync::Mutex::new(None)),
            start_version,
            shared_fence_pick: rand::random(),
        })
    }

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Clone + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send + std::fmt::Debug,
    {
        let mut retries = 0;
        let mut base_delay_ms = 50u64;

        loop {
            let f_clone = f.clone();
            let tx = self
                .begin_transaction()
                .await
                .map_err(|e| {
                    TitoError::TransactionFailed(format!("Failed to begin transaction: {}", e))
                })
                .map_err(E::from)?;

            let mut registration = TransactionRegistration {
                backend: self.clone(),
                transaction: Some(tx.clone()),
            };

            {
                let mut active_transactions = self.active_transactions.lock().await;
                active_transactions.insert(tx.id.clone(), tx.clone());
            }

            let result = f_clone(tx.clone()).await;

            match result {
                Ok(value) => match tx.clone().commit().await {
                    Ok(_) => {
                        registration.finish().await;
                        return Ok(value);
                    }
                    Err(e) => {
                        if !e.is_commit_outcome_unknown() {
                            let _ = tx.rollback().await;
                        }
                        registration.finish().await;

                        if commit_failure_action(&e, retries) == CommitFailureAction::Retry {
                            retries += 1;
                            let jitter = rand::thread_rng().gen_range(0..base_delay_ms / 2);
                            let delay = base_delay_ms + jitter;
                            sleep(Duration::from_millis(delay)).await;
                            base_delay_ms = (base_delay_ms * 2).min(2000);
                            continue;
                        }
                        return Err(E::from(e));
                    }
                },
                Err(e) => {
                    let is_retryable = tx.had_retryable_error.load(Ordering::Relaxed);
                    let _ = tx.rollback().await;
                    registration.finish().await;

                    if is_retryable && retries < MAX_TRANSACTION_RETRIES {
                        retries += 1;
                        let jitter = rand::thread_rng().gen_range(0..base_delay_ms / 2);
                        let delay = base_delay_ms + jitter;
                        sleep(Duration::from_millis(delay)).await;
                        base_delay_ms = (base_delay_ms * 2).min(2000);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        let mut active_transactions = self.active_transactions.lock().await;
        let transactions: Vec<_> = active_transactions.drain().collect();
        drop(active_transactions);

        for (_tx_id, tx) in transactions {
            if !tx.commit_started.load(Ordering::Acquire) {
                let _ = tx.rollback().await;
            }
        }
        Ok(())
    }

    async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError> {
        validate_delete_range(start, end)?;
        let range: tikv_client::BoundRange = (start.to_vec()..end.to_vec()).into();

        self.client
            .unsafe_destroy_range(range)
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete range failed: {}", e)))?;

        Ok(())
    }

    async fn garbage_collect(&self, retention: Duration) -> Result<(), TitoError> {
        if retention.is_zero() {
            return Err(TitoError::InvalidInput(
                "Garbage collection retention must be greater than zero".to_string(),
            ));
        }

        let current = self.client.current_timestamp().await.map_err(|e| {
            TitoError::TransactionFailed(format!(
                "Failed to read timestamp for garbage collection: {}",
                e
            ))
        })?;
        let safepoint = gc_safepoint(current, retention);

        self.client
            .gc(safepoint)
            .await
            .map(|_| ())
            .map_err(|e| TitoError::TransactionFailed(format!("Garbage collection failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{MAX_QUEUE_EVENT_BYTES, QUEUE_SCAN_RPC_LIMIT};

    #[test]
    fn a_timestamp_request_the_client_could_not_send_is_retried_even_at_commit() {
        let flag = AtomicBool::new(false);
        let error = classify_tikv_error(
            tikv_client::Error::InternalError {
                message: TIMESTAMP_CHANNEL_CLOSED.to_string(),
            },
            "Transaction commit failed",
            &flag,
        );
        assert!(matches!(error, TitoError::Retryable(_)));
        assert_eq!(commit_failure_action(&error, 0), CommitFailureAction::Retry);
        let other = classify_tikv_error(
            tikv_client::Error::InternalError {
                message: "PD gives more TsoResponse than expected".to_string(),
            },
            "Transaction commit failed",
            &flag,
        );
        assert!(matches!(other, TitoError::TransactionFailed(_)));
    }

    #[tokio::test]
    async fn a_timestamp_request_whose_answer_never_came_is_retried_even_at_commit() {
        let (answer, wait) = tokio::sync::oneshot::channel::<()>();
        drop(answer);
        let dropped = wait.await.expect_err("A dropped answer ends the wait");
        let error = classify_tikv_error(
            tikv_client::Error::Channel(dropped),
            "Transaction commit failed",
            &AtomicBool::new(false),
        );
        assert!(matches!(error, TitoError::Retryable(_)));
        assert_eq!(commit_failure_action(&error, 0), CommitFailureAction::Retry);
    }

    #[test]
    fn queue_scan_pages_have_two_x_tikv_decode_headroom() {
        let maximum_valid_invocation_bytes = MAX_QUEUE_EVENT_BYTES * QUEUE_SCAN_RPC_LIMIT as usize;

        assert_eq!(maximum_valid_invocation_bytes, 16 * 1024 * 1024);
        assert!(
            TIKV_GRPC_MAX_DECODING_MESSAGE_BYTES >= maximum_valid_invocation_bytes * 2,
            "TiKV decoding must leave headroom for terminal metadata, keys, and protobuf framing"
        );
    }

    #[test]
    fn scan_rpcs_are_sized_by_the_largest_row_of_the_previous_page() {
        assert_eq!(scan_rows_for(MAX_QUEUE_EVENT_BYTES), 4);
        assert_eq!(scan_rows_for(MAX_QUEUE_EVENT_BYTES + 1), 3);
        assert_eq!(scan_rows_for(SCAN_RPC_BYTES), 1);
        assert_eq!(scan_rows_for(8 * SCAN_RPC_BYTES), 1);
        assert_eq!(scan_rows_for(0), SCAN_RPC_MAX_ROWS);
        assert_eq!(scan_rows_for(200), SCAN_RPC_MAX_ROWS);
        assert_eq!(scan_rows_for(2_048), 2_048);
        assert!(
            SCAN_RPC_FIRST_ROWS as usize * MAX_QUEUE_EVENT_BYTES * 2
                <= TIKV_GRPC_MAX_DECODING_MESSAGE_BYTES
        );
        assert!(SCAN_RPC_BYTES * 2 <= TIKV_GRPC_MAX_DECODING_MESSAGE_BYTES);
    }

    #[test]
    fn undetermined_commit_is_classified_as_unknown_and_never_retried() {
        let retryable_flag = AtomicBool::new(false);
        let error = classify_tikv_error(
            tikv_client::Error::UndeterminedError(Box::new(tikv_client::Error::Unimplemented)),
            "Transaction commit failed",
            &retryable_flag,
        );

        assert!(matches!(error, TitoError::CommitOutcomeUnknown(_)));
        assert!(!error.is_retryable());
        assert!(!retryable_flag.load(Ordering::Relaxed));
        assert_eq!(
            commit_failure_action(&error, 0),
            CommitFailureAction::Return
        );
    }

    #[test]
    fn commit_retry_policy_only_retries_explicit_retryable_errors_within_budget() {
        let retryable = TitoError::Retryable("conflict".to_string());

        assert_eq!(
            commit_failure_action(&retryable, 0),
            CommitFailureAction::Retry
        );
        assert_eq!(
            commit_failure_action(&retryable, MAX_TRANSACTION_RETRIES),
            CommitFailureAction::Return
        );
        assert_eq!(
            commit_failure_action(
                &TitoError::TransactionFailed("determined failure".to_string()),
                0
            ),
            CommitFailureAction::Return
        );
    }

    #[test]
    fn gc_safepoint_subtracts_retention_in_milliseconds_and_saturates() {
        let current = Timestamp {
            physical: 1_800_000_000_000,
            logical: 42,
            ..Default::default()
        };

        let retained = gc_safepoint(current, Duration::from_secs(24 * 60 * 60));
        assert_eq!(retained.physical, 1_799_913_600_000);
        assert_eq!(retained.logical, 0);

        let saturated = gc_safepoint(
            Timestamp {
                physical: 1_000,
                logical: 42,
                ..Default::default()
            },
            Duration::from_millis(2_000),
        );
        assert_eq!(saturated.physical, 0);
        assert_eq!(saturated.logical, 0);
    }
}

#[derive(Clone)]
pub struct TiKVTransaction {
    pub id: String,
    pub inner: Arc<tokio::sync::Mutex<Transaction>>,
    snapshot: Arc<tokio::sync::Mutex<Snapshot>>,
    client: Arc<TransactionClient>,
    pub had_retryable_error: Arc<AtomicBool>,
    commit_started: Arc<AtomicBool>,
    wrote: Arc<AtomicBool>,
    guards: Arc<tokio::sync::Mutex<Guards>>,
    records: Arc<std::sync::Mutex<HashMap<String, Vec<RememberedEntry>>>>,
    written: Arc<std::sync::Mutex<BTreeSet<Vec<u8>>>>,
    missing: Arc<std::sync::Mutex<HashSet<Vec<u8>>>>,
    inserts: Arc<std::sync::Mutex<HashMap<Vec<u8>, TitoError>>>,
    refused: Arc<std::sync::Mutex<Option<TitoError>>>,
    start_version: u64,
    shared_fence_pick: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanType {
    Forward,
    Reverse,
    Keys,
}

impl ScanType {
    fn failure(self) -> &'static str {
        match self {
            Self::Forward | Self::Keys => "Scan operation failed",
            Self::Reverse => "Reverse scan operation failed",
        }
    }
}

impl TiKVTransaction {
    async fn scan_rpc(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u32,
        r#type: ScanType,
    ) -> Result<Vec<TitoKvPair>, tikv_client::Error> {
        let range: tikv_client::BoundRange = (start.to_vec()..end.to_vec()).into();
        let written = self.written_between(start, end);
        if written.is_empty() {
            let mut transaction = self.inner.lock().await;
            return match r#type {
                ScanType::Keys => Ok(transaction
                    .scan_keys(range, limit)
                    .await?
                    .map(|key| (key.into(), Vec::new()))
                    .collect()),
                ScanType::Forward => Ok(transaction
                    .scan(range, limit)
                    .await?
                    .map(|kv| (kv.0.into(), kv.1))
                    .collect()),
                ScanType::Reverse => Ok(transaction
                    .scan_reverse(range, limit)
                    .await?
                    .map(|kv| (kv.0.into(), kv.1))
                    .collect()),
            };
        }
        let visible: BTreeMap<Vec<u8>, Vec<u8>> = self
            .inner
            .lock()
            .await
            .batch_get(written.iter().cloned().collect::<Vec<_>>())
            .await?
            .map(|kv| (kv.0.into(), kv.1))
            .collect();
        let hidden = u32::try_from(written.len() - visible.len()).unwrap_or(u32::MAX);
        let fetch = limit.saturating_add(hidden);
        let stored: Vec<TitoKvPair> = {
            let mut snapshot = self.snapshot.lock().await;
            match r#type {
                ScanType::Keys => snapshot
                    .scan_keys(range, fetch)
                    .await?
                    .map(|key| (key.into(), Vec::new()))
                    .collect(),
                ScanType::Forward => snapshot
                    .scan(range, fetch)
                    .await?
                    .map(|kv| (kv.0.into(), kv.1))
                    .collect(),
                ScanType::Reverse => snapshot
                    .scan_reverse(range, fetch)
                    .await?
                    .map(|kv| (kv.0.into(), kv.1))
                    .collect(),
            }
        };
        let mut rows: BTreeMap<Vec<u8>, Vec<u8>> = stored
            .into_iter()
            .filter(|(key, _)| !written.contains(key))
            .collect();
        for (key, value) in visible {
            let value = match r#type {
                ScanType::Keys => Vec::new(),
                ScanType::Forward | ScanType::Reverse => value,
            };
            rows.insert(key, value);
        }
        let limit = limit as usize;
        Ok(match r#type {
            ScanType::Reverse => rows.into_iter().rev().take(limit).collect(),
            ScanType::Forward | ScanType::Keys => rows.into_iter().take(limit).collect(),
        })
    }

    fn written_between(&self, start: &[u8], end: &[u8]) -> BTreeSet<Vec<u8>> {
        self.written
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .range(start.to_vec()..end.to_vec())
            .cloned()
            .collect()
    }

    fn note_key(&self, key: &[u8]) {
        self.written
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.to_vec());
        self.missing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
    }

    fn written_span(&self, guards: &[Vec<u8>]) -> Option<(Vec<u8>, Vec<u8>)> {
        let written = self
            .written
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let first = written.first().into_iter().chain(guards).min()?.clone();
        let last = written.last().into_iter().chain(guards).max()?;
        Some((first, key_after_bytes(last)))
    }

    async fn clear_expired_locks(&self, owners: Vec<u64>, start: &[u8], end: &[u8]) {
        if owners.is_empty() {
            return;
        }
        let Ok(now) = self.client.current_timestamp().await else {
            return;
        };
        for owner in owners.into_iter().collect::<BTreeSet<_>>() {
            let began = Timestamp::from_version(owner);
            if now.physical < began.physical.saturating_add(SHORTEST_LOCK_LIFETIME_MS) {
                continue;
            }
            let Ok(locks) = self
                .client
                .scan_locks(&began, start.to_vec()..end.to_vec(), LOCK_SCAN_LIMIT)
                .await
            else {
                continue;
            };
            let Some(lock) = locks
                .into_iter()
                .find(|lock| lock.lock_version == owner && lock_expired(lock, &now))
            else {
                continue;
            };
            let after = key_after_bytes(&lock.key);
            let _ = self
                .client
                .cleanup_locks(lock.key..after, &began, ResolveLocksOptions::default())
                .await;
        }
    }

    async fn scan_pages(
        &self,
        mut start: Vec<u8>,
        mut end: Vec<u8>,
        limit: u32,
        r#type: ScanType,
        first_rpc_rows: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let mut rows: Vec<TitoKvPair> = Vec::new();
        let mut rpc_rows = first_rpc_rows;
        while start < end {
            let remaining = limit.saturating_sub(rows.len() as u32);
            if remaining == 0 {
                break;
            }
            let requested = remaining.min(rpc_rows);
            let page = match self.scan_rpc(&start, &end, requested, r#type).await {
                Ok(page) => page,
                Err(error) if requested > 1 && exceeds_message_limit(&error) => {
                    rpc_rows = requested / 2;
                    continue;
                }
                Err(error) => {
                    let owners = missing_lock_owners(&error);
                    self.clear_expired_locks(owners, &start, &end).await;
                    return Err(classify_tikv_error(
                        error,
                        r#type.failure(),
                        &self.had_retryable_error,
                    ));
                }
            };
            let full = page.len() >= requested as usize;
            let Some((last, _)) = page.last() else {
                break;
            };
            match r#type {
                ScanType::Forward | ScanType::Keys => start = key_after_bytes(last),
                ScanType::Reverse => end = last.clone(),
            }
            rpc_rows = scan_rows_for(
                page.iter()
                    .map(|(key, value)| key.len() + value.len())
                    .fold(0, usize::max),
            );
            rows.extend(page);
            if !full {
                break;
            }
        }
        Ok(rows)
    }

    pub(crate) async fn remember(&self, records: Vec<Noted>, guard: Guard) {
        if guard != Guard::None {
            let mut guards = self.guards.lock().await;
            for record in &records {
                note_guard(&mut guards, &record.table, &record.id, guard);
            }
        }
        let mut memory = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for record in records {
            memory.insert(record.key, record.entries);
        }
    }

    pub(crate) fn remembered(&self, key: &str) -> Option<Vec<RememberedEntry>> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
            .cloned()
    }

    pub(crate) fn note_written(&self, key: String, entries: Vec<RememberedEntry>) {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, entries);
    }

    pub(crate) fn forget(&self, key: &str) {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
    }

    pub(crate) async fn insert(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        taken: TitoError,
    ) -> Result<(), TitoError> {
        let outcome = self.inner.lock().await.insert(key.clone(), value).await;
        match outcome {
            Ok(()) => {}
            Err(tikv_client::Error::DuplicateKeyInsertion) => {
                let mut refused = self
                    .refused
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if refused.is_none() {
                    *refused = Some(taken.clone());
                }
                return Err(taken);
            }
            Err(error) => {
                return Err(classify_tikv_error(
                    error,
                    "Insert operation failed",
                    &self.had_retryable_error,
                ))
            }
        }
        note_write(&mut *self.guards.lock().await, &key);
        self.note_key(&key);
        self.wrote.store(true, Ordering::Release);
        self.inserts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, taken);
        Ok(())
    }

    fn taken(&self, key: &[u8]) -> Option<TitoError> {
        self.inserts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
            .cloned()
    }

    pub(crate) async fn fetch(&self, requests: Vec<Request>) -> Result<Vec<Rows>, TitoError> {
        let mut keys = Vec::new();
        let mut seen = HashSet::new();
        let mut scans = Vec::new();
        for request in &requests {
            match request {
                Request::Keys(wanted) => {
                    for key in wanted {
                        if seen.insert(key.clone()) {
                            keys.push(key.clone());
                        }
                    }
                }
                Request::Scan(scan) => {
                    if scan.start < scan.end {
                        scans.push(scan.clone());
                    }
                }
            }
        }
        let found: HashMap<Vec<u8>, Vec<u8>> = self.read_keys(keys).await?.into_iter().collect();
        let mut pages = Vec::with_capacity(scans.len());
        for scan in scans {
            let r#type = if scan.reverse {
                ScanType::Reverse
            } else {
                ScanType::Forward
            };
            pages.push(
                self.scan_pages(
                    scan.start,
                    scan.end,
                    scan.limit,
                    r#type,
                    SCAN_RPC_FIRST_ROWS,
                )
                .await?,
            );
        }
        let mut pages = pages.into_iter();
        let mut rows = Vec::with_capacity(requests.len());
        for request in requests {
            rows.push(match request {
                Request::Keys(wanted) => {
                    Rows::Keys(wanted.iter().map(|key| found.get(key).cloned()).collect())
                }
                Request::Scan(scan) if scan.start < scan.end => {
                    Rows::Scan(pages.next().unwrap_or_default())
                }
                Request::Scan(_) => Rows::Scan(Vec::new()),
            });
        }
        Ok(rows)
    }

    async fn read_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<TitoKvPair>, TitoError> {
        let asked = {
            let missing = self
                .missing
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            keys.into_iter()
                .filter(|key| !missing.contains(key))
                .collect::<Vec<_>>()
        };
        if asked.is_empty() {
            return Ok(Vec::new());
        }
        let mut chunks = vec![asked.clone()];
        let mut found = Vec::new();
        let mut attempt = 0;
        while let Some(mut chunk) = chunks.pop() {
            let request: Vec<tikv_client::Key> = chunk.iter().cloned().map(Into::into).collect();
            let result = self
                .inner
                .lock()
                .await
                .batch_get(request)
                .await
                .map(|pairs| {
                    pairs
                        .map(|kv| (kv.0.into(), kv.1))
                        .collect::<Vec<TitoKvPair>>()
                });
            match result {
                Ok(pairs) => found.extend(pairs),
                Err(error) if chunk.len() > 1 && exceeds_message_limit(&error) => {
                    let second = chunk.split_off(chunk.len() / 2);
                    chunks.push(second);
                    chunks.push(chunk);
                }
                Err(error) => {
                    let owners = missing_lock_owners(&error);
                    if let (Some(first), Some(last)) = (chunk.iter().min(), chunk.iter().max()) {
                        let end = key_after_bytes(last);
                        self.clear_expired_locks(owners, first, &end).await;
                    }
                    let pending = AtomicBool::new(false);
                    let error = classify_tikv_error(error, "Batch get operation failed", &pending);
                    attempt += 1;
                    if !error.is_retryable() || attempt >= MAX_READ_LOCK_RESOLUTION_ATTEMPTS {
                        if pending.load(Ordering::Relaxed) {
                            self.had_retryable_error.store(true, Ordering::Relaxed);
                        }
                        return Err(error);
                    }
                    sleep(READ_LOCK_RESOLUTION_BACKOFF * attempt).await;
                    chunks.push(chunk);
                }
            }
        }
        {
            let present = found.iter().map(|(key, _)| key).collect::<HashSet<_>>();
            self.missing
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .extend(asked.into_iter().filter(|key| !present.contains(key)));
        }
        Ok(found)
    }
}

#[async_trait]
impl TitoTransaction for TiKVTransaction {
    fn start_version(&self) -> u64 {
        self.start_version
    }

    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError> {
        let known_missing = self
            .missing
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(key.as_ref());
        if known_missing {
            return Ok(None);
        }
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        let mut attempt = 0;
        loop {
            let outcome = self.inner.lock().await.get(tikv_key.clone()).await;
            match outcome {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let owners = missing_lock_owners(&error);
                    let start: Vec<u8> = tikv_key.clone().into();
                    let end = key_after_bytes(&start);
                    self.clear_expired_locks(owners, &start, &end).await;
                    let pending = AtomicBool::new(false);
                    let error = classify_tikv_error(error, "Get operation failed", &pending);
                    attempt += 1;
                    if !error.is_retryable() || attempt >= MAX_READ_LOCK_RESOLUTION_ATTEMPTS {
                        if pending.load(Ordering::Relaxed) {
                            self.had_retryable_error.store(true, Ordering::Relaxed);
                        }
                        return Err(error);
                    }
                    sleep(READ_LOCK_RESOLUTION_BACKOFF * attempt).await;
                }
            }
        }
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), TitoError> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        let value_bytes = value.as_ref().to_vec();
        self.inner
            .lock()
            .await
            .put(tikv_key, value_bytes)
            .await
            .map_err(|e| {
                classify_tikv_error(e, "Put operation failed", &self.had_retryable_error)
            })?;
        note_write(&mut *self.guards.lock().await, key.as_ref());
        self.note_key(key.as_ref());
        self.wrote.store(true, Ordering::Release);
        Ok(())
    }

    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), TitoError> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        self.inner
            .lock()
            .await
            .delete(tikv_key)
            .await
            .map_err(|e| {
                classify_tikv_error(e, "Delete operation failed", &self.had_retryable_error)
            })?;
        note_write(&mut *self.guards.lock().await, key.as_ref());
        self.note_key(key.as_ref());
        self.wrote.store(true, Ordering::Release);
        Ok(())
    }

    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        self.scan_pages(
            range.start.as_ref().to_vec(),
            range.end.as_ref().to_vec(),
            limit,
            ScanType::Forward,
            SCAN_RPC_FIRST_ROWS,
        )
        .await
    }

    async fn scan_keys<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, TitoError> {
        Ok(self
            .scan_pages(
                range.start.as_ref().to_vec(),
                range.end.as_ref().to_vec(),
                limit,
                ScanType::Keys,
                SCAN_RPC_FIRST_ROWS,
            )
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }

    async fn scan_reverse<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        self.scan_pages(
            range.start.as_ref().to_vec(),
            range.end.as_ref().to_vec(),
            limit,
            ScanType::Reverse,
            SCAN_RPC_FIRST_ROWS,
        )
        .await
    }

    async fn get_keys<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
        _sealed: Sealed,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        self.read_keys(keys.iter().map(|key| key.as_ref().to_vec()).collect())
            .await
    }

    async fn commit(self) -> Result<(), TitoError> {
        let mut transaction = self.inner.lock().await;
        let refused = self
            .refused
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(refused) = refused {
            let _ = transaction.rollback().await;
            return Err(refused);
        }
        let keys: Vec<Vec<u8>> = guard_keys(
            &*self.guards.lock().await,
            self.wrote.load(Ordering::Acquire),
            self.shared_fence_pick,
        )
        .into_iter()
        .map(String::into_bytes)
        .collect();
        for key in &keys {
            transaction.delete(key.clone()).await.map_err(|e| {
                classify_tikv_error(e, "Guard delete failed", &self.had_retryable_error)
            })?;
        }
        self.commit_started.store(true, Ordering::Release);
        match transaction.commit().await {
            Ok(_) => Ok(()),
            Err(error) => {
                if let Some(taken) = already_exist_key(&error).and_then(|key| self.taken(&key)) {
                    return Err(taken);
                }
                if contains_grpc_error(&error) {
                    return Err(TitoError::CommitOutcomeUnknown(format!(
                        "Transaction commit failed: {}",
                        error
                    )));
                }
                let owners = missing_lock_owners(&error);
                if let Some((first, end)) = self.written_span(&keys) {
                    self.clear_expired_locks(owners, &first, &end).await;
                }
                Err(classify_tikv_error(
                    error,
                    "Transaction commit failed",
                    &self.had_retryable_error,
                ))
            }
        }
    }

    async fn rollback(self) -> Result<(), TitoError> {
        self.inner.lock().await.rollback().await.map_err(|e| {
            classify_tikv_error(e, "Transaction rollback failed", &self.had_retryable_error)
        })
    }
}

pub struct TiKV;

impl TiKV {
    pub async fn connect<S: AsRef<str>>(endpoints: Vec<S>) -> Result<TiKVBackend, TitoError> {
        let endpoint_strings: Vec<String> =
            endpoints.iter().map(|s| s.as_ref().to_string()).collect();
        let client_config = tikv_client::Config::default()
            .with_grpc_max_decoding_message_size(TIKV_GRPC_MAX_DECODING_MESSAGE_BYTES);
        let client = TransactionClient::new_with_config(endpoint_strings, client_config)
            .await
            .map_err(|e| {
                TitoError::ConnectionFailed(format!("Failed to connect to TiKV: {}", e))
            })?;

        Ok(TiKVBackend {
            client: Arc::new(client),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}
