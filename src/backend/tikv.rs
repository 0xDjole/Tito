use crate::error::TitoError;
use crate::fence::{guard_keys, note_guard, Guards};
use crate::types::{
    validate_delete_range, DBUuid, TitoEngine, TitoGuard, TitoKvPair, TitoTransaction, TitoValue,
};
use crate::utils::key_after_bytes;
use async_trait::async_trait;
use futures::lock::Mutex;
use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tikv_client::{Timestamp, TimestampExt, Transaction, TransactionClient};
use tokio::time::sleep;

pub(crate) const MAX_TRANSACTION_RETRIES: u32 = 10;
const MAX_READ_LOCK_RESOLUTION_ATTEMPTS: u32 = 8;
const READ_LOCK_RESOLUTION_BACKOFF: Duration = Duration::from_millis(25);
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

fn classify_tikv_error(
    e: tikv_client::Error,
    context: &str,
    retryable_flag: &AtomicBool,
) -> TitoError {
    let msg = format!("{}: {}", context, e);
    if contains_undetermined_error(&e) {
        return TitoError::CommitOutcomeUnknown(msg);
    }

    let is_retryable = match &e {
        tikv_client::Error::RegionError(_)
        | tikv_client::Error::RegionForKeyNotFound { .. }
        | tikv_client::Error::RegionForRangeNotFound { .. }
        | tikv_client::Error::RegionNotFoundInResponse { .. }
        | tikv_client::Error::LeaderNotFound { .. }
        | tikv_client::Error::ResolveLockError(_)
        | tikv_client::Error::TxnNotFound(_)
        | tikv_client::Error::NoCurrentRegions
        | tikv_client::Error::EntryNotFoundInRegionCache
        | tikv_client::Error::PessimisticLockError { .. } => true,
        tikv_client::Error::KeyError(ke) => {
            ke.locked.is_some() || ke.conflict.is_some() || !ke.retryable.is_empty()
        }
        tikv_client::Error::MultipleKeyErrors(errs) | tikv_client::Error::ExtractedErrors(errs) => {
            errs.iter().any(|inner| {
                matches!(
                    inner,
                    tikv_client::Error::RegionError(_)
                        | tikv_client::Error::ResolveLockError(_)
                        | tikv_client::Error::TxnNotFound(_)
                        | tikv_client::Error::LeaderNotFound { .. }
                        | tikv_client::Error::RegionForKeyNotFound { .. }
                        | tikv_client::Error::KeyError(_)
                )
            })
        }
        _ => false,
    };
    if is_retryable {
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
        let no_flag = AtomicBool::new(false);
        let tx = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| classify_tikv_error(e, "Failed to begin transaction", &no_flag))?;
        let start_version = tx.start_timestamp().version();
        Ok(TiKVTransaction {
            id: DBUuid::new_v4().to_string(),
            inner: Arc::new(tokio::sync::Mutex::new(tx)),
            had_retryable_error: Arc::new(AtomicBool::new(false)),
            commit_started: Arc::new(AtomicBool::new(false)),
            wrote: Arc::new(AtomicBool::new(false)),
            guards: Arc::new(tokio::sync::Mutex::new(Guards::new())),
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
    pub had_retryable_error: Arc<AtomicBool>,
    commit_started: Arc<AtomicBool>,
    wrote: Arc<AtomicBool>,
    guards: Arc<tokio::sync::Mutex<Guards>>,
    start_version: u64,
    shared_fence_pick: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanDirection {
    Forward,
    Reverse,
}

impl ScanDirection {
    fn failure(self) -> &'static str {
        match self {
            Self::Forward => "Scan operation failed",
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
        direction: ScanDirection,
        keys_only: bool,
    ) -> Result<Vec<TitoKvPair>, tikv_client::Error> {
        let range: tikv_client::BoundRange = (start.to_vec()..end.to_vec()).into();
        let mut transaction = self.inner.lock().await;
        if keys_only {
            return match direction {
                ScanDirection::Forward => Ok(transaction
                    .scan_keys(range, limit)
                    .await?
                    .map(|key| (key.into(), Vec::new()))
                    .collect()),
                ScanDirection::Reverse => Ok(transaction
                    .scan_keys_reverse(range, limit)
                    .await?
                    .map(|key| (key.into(), Vec::new()))
                    .collect()),
            };
        }
        match direction {
            ScanDirection::Forward => Ok(transaction
                .scan(range, limit)
                .await?
                .map(|kv| (kv.0.into(), kv.1))
                .collect()),
            ScanDirection::Reverse => Ok(transaction
                .scan_reverse(range, limit)
                .await?
                .map(|kv| (kv.0.into(), kv.1))
                .collect()),
        }
    }

    async fn scan_pages(
        &self,
        mut start: Vec<u8>,
        mut end: Vec<u8>,
        limit: u32,
        direction: ScanDirection,
        keys_only: bool,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let mut rows: Vec<TitoKvPair> = Vec::new();
        let mut rpc_rows = SCAN_RPC_FIRST_ROWS;
        while start < end {
            let remaining = limit.saturating_sub(rows.len() as u32);
            if remaining == 0 {
                break;
            }
            let requested = remaining.min(rpc_rows);
            let page = match self
                .scan_rpc(&start, &end, requested, direction, keys_only)
                .await
            {
                Ok(page) => page,
                Err(error) if requested > 1 && exceeds_message_limit(&error) => {
                    rpc_rows = requested / 2;
                    continue;
                }
                Err(error) => {
                    return Err(classify_tikv_error(
                        error,
                        direction.failure(),
                        &self.had_retryable_error,
                    ))
                }
            };
            let full = page.len() >= requested as usize;
            let Some((last, _)) = page.last() else {
                break;
            };
            match direction {
                ScanDirection::Forward => start = key_after_bytes(last),
                ScanDirection::Reverse => end = last.clone(),
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
}

#[async_trait]
impl TitoTransaction for TiKVTransaction {
    fn start_version(&self) -> u64 {
        self.start_version
    }

    async fn guard(&self, table: &str, id: &str, guard: TitoGuard) {
        note_guard(&mut *self.guards.lock().await, table, id, guard);
    }

    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        let mut attempt = 0;
        loop {
            let pending = AtomicBool::new(false);
            let outcome = self
                .inner
                .lock()
                .await
                .get(tikv_key.clone())
                .await
                .map_err(|e| classify_tikv_error(e, "Get operation failed", &pending));
            match outcome {
                Ok(value) => return Ok(value),
                Err(error) => {
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
            ScanDirection::Forward,
            false,
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
                ScanDirection::Forward,
                true,
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
            ScanDirection::Reverse,
            false,
        )
        .await
    }

    async fn batch_get<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let mut chunks = vec![keys
            .iter()
            .map(|key| key.as_ref().to_vec())
            .collect::<Vec<_>>()];
        let mut found = Vec::new();
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
                    return Err(classify_tikv_error(
                        error,
                        "Batch get operation failed",
                        &self.had_retryable_error,
                    ))
                }
            }
        }
        Ok(found)
    }

    async fn commit(self) -> Result<(), TitoError> {
        let mut transaction = self.inner.lock().await;
        let keys = guard_keys(
            &*self.guards.lock().await,
            self.wrote.load(Ordering::Acquire),
            self.shared_fence_pick,
        );
        for key in keys {
            transaction.delete(key.into_bytes()).await.map_err(|e| {
                classify_tikv_error(e, "Guard delete failed", &self.had_retryable_error)
            })?;
        }
        self.commit_started.store(true, Ordering::Release);
        transaction.commit().await.map(|_| ()).map_err(|e| {
            classify_tikv_error(e, "Transaction commit failed", &self.had_retryable_error)
        })
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
