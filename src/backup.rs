use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Write};

use crate::error::TitoError;
use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::utils::next_string_lexicographically;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChangelogEntry {
    pub op: String,
    pub key: String,
    pub data: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventChangelogEntry {
    pub event_id: String,
    pub event_key: String,
    pub timestamp: i64,
    pub data: Value,
}

#[async_trait]
pub trait BackupStorage: Send + Sync {
    type Error: std::fmt::Display + Send + Sync;
    async fn upload(&self, key: &str, data: &[u8]) -> Result<(), Self::Error>;
    async fn download(&self, key: &str) -> Result<Vec<u8>, Self::Error>;
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, Self::Error>;
    async fn delete_keys(&self, keys: Vec<String>) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub chunk_size: u32,
    pub full_backup_interval_secs: i64,
    pub retention_secs: i64,
    pub backup_root: String,
}

#[derive(Debug)]
pub struct BackupResult {
    pub state_changelog_entries: usize,
    pub event_changelog_entries: usize,
    pub full_backup_entries: Option<usize>,
}

#[derive(Debug)]
pub struct RestoreResult {
    pub state_entries_restored: usize,
    pub events_replayed: usize,
}

#[derive(Clone)]
pub struct TitoBackupService<E: TitoEngine, S: BackupStorage> {
    engine: E,
    storage: Arc<S>,
    config: BackupConfig,
}

impl<E: TitoEngine, S: BackupStorage + 'static> TitoBackupService<E, S> {
    pub fn new(engine: E, storage: Arc<S>, config: BackupConfig) -> Self {
        Self {
            engine,
            storage,
            config,
        }
    }

    pub async fn run_backup(&self) -> Result<BackupResult, TitoError> {
        let state_changelog_entries = self.ship_state_changelog().await?;
        let event_changelog_entries = self.ship_event_changelog().await?;
        let full_backup_entries = self.check_and_run_full_state_backup().await?;
        Ok(BackupResult {
            state_changelog_entries,
            event_changelog_entries,
            full_backup_entries,
        })
    }

    pub async fn ship_state_changelog(&self) -> Result<usize, TitoError> {
        let root = &self.config.backup_root;
        let timestamp = Utc::now().timestamp();
        let timestamp_ms = Utc::now().timestamp_millis();
        let chunk_size = self.config.chunk_size;

        let latest_key = format!("{}/LATEST_STATE_PARTIAL", root);
        let start_ts = self.download_marker_timestamp(&latest_key).await;

        let prefixes = vec!["state_changelog:", "changelog:"];
        let mut total_entries = 0;
        let mut seq = 0;

        for prefix in prefixes {
            let start_key = match start_ts {
                Some(ts) => {
                    let ts_ms = if ts < 1_000_000_000_000 {
                        ts * 1000
                    } else {
                        ts
                    };
                    format!("{}{}:", prefix, ts_ms + 1)
                }
                None => prefix.to_string(),
            };

            let mut current_key = start_key;
            let mut all_keys_to_delete: Vec<String> = Vec::new();

            loop {
                let scan_key = current_key.clone();
                let prefix_owned = prefix.to_string();
                let (entries, last_key) = self
                    .engine
                    .clone()
                    .transaction(|tx| {
                        let scan_key = scan_key.clone();
                        let prefix_owned = prefix_owned.clone();
                        async move {
                            scan_changelog_range(&scan_key, &prefix_owned, chunk_size, &tx).await
                        }
                    })
                    .await?;

                if entries.is_empty() {
                    break;
                }

                let entry_count = entries.len();
                let changelog_keys: Vec<String> = entries.iter().map(|(k, _)| k.clone()).collect();
                let changelog_entries: Vec<_> =
                    entries.into_iter().map(|(_, entry)| entry).collect();

                let chunk_key = format!(
                    "{}/state/partial/{}/chunk-{:04}.json.gz",
                    root, timestamp, seq
                );
                self.compress_and_upload_changelog(&changelog_entries, &chunk_key)
                    .await?;

                all_keys_to_delete.extend(changelog_keys);
                total_entries += entry_count;
                seq += 1;

                if let Some(lk) = last_key {
                    current_key = next_string_lexicographically(lk);
                } else {
                    break;
                }
            }

            if !all_keys_to_delete.is_empty() {
                for batch in all_keys_to_delete.chunks(chunk_size as usize) {
                    let keys = batch.to_vec();
                    self.engine
                        .clone()
                        .transaction(|tx| {
                            let keys = keys.clone();
                            async move {
                                for key in keys {
                                    tx.delete(key.as_bytes()).await.map_err(|e| {
                                        TitoError::BackupFailed(format!(
                                            "Delete changelog entry: {}",
                                            e
                                        ))
                                    })?;
                                }
                                Ok::<_, TitoError>(())
                            }
                        })
                        .await?;
                }
            }
        }

        if total_entries > 0 {
            self.upload_marker(&format!("{}/LATEST_STATE_PARTIAL", root), timestamp_ms)
                .await?;
        }

        Ok(total_entries)
    }

    pub async fn ship_event_changelog(&self) -> Result<usize, TitoError> {
        let root = &self.config.backup_root;
        let timestamp = Utc::now().timestamp();
        let timestamp_ms = Utc::now().timestamp_millis();
        let chunk_size = self.config.chunk_size;

        let latest_key = format!("{}/LATEST_EVENTS", root);
        let start_key = match self.download_marker_timestamp(&latest_key).await {
            Some(ts) => {
                let ts_ms = if ts < 1_000_000_000_000 {
                    ts * 1000
                } else {
                    ts
                };
                format!("event_changelog:{}:", ts_ms + 1)
            }
            None => "event_changelog:".to_string(),
        };

        let mut current_key = start_key;
        let mut seq = 0;
        let mut total_entries = 0;
        let mut all_keys_to_delete: Vec<String> = Vec::new();

        loop {
            let scan_key = current_key.clone();
            let (entries, last_key) = self
                .engine
                .clone()
                .transaction(|tx| {
                    let scan_key = scan_key.clone();
                    async move { scan_event_changelog_range(&scan_key, chunk_size, &tx).await }
                })
                .await?;

            if entries.is_empty() {
                break;
            }

            let entry_count = entries.len();
            let changelog_keys: Vec<String> = entries.iter().map(|(k, _)| k.clone()).collect();
            let changelog_entries: Vec<_> = entries.into_iter().map(|(_, entry)| entry).collect();

            let chunk_key = format!("{}/events/{}/chunk-{:04}.json.gz", root, timestamp, seq);
            self.compress_and_upload_events(&changelog_entries, &chunk_key)
                .await?;

            all_keys_to_delete.extend(changelog_keys);
            total_entries += entry_count;
            seq += 1;

            if let Some(lk) = last_key {
                current_key = next_string_lexicographically(lk);
            } else {
                break;
            }
        }

        if total_entries > 0 {
            self.upload_marker(&format!("{}/LATEST_EVENTS", root), timestamp_ms)
                .await?;

            for batch in all_keys_to_delete.chunks(chunk_size as usize) {
                let keys = batch.to_vec();
                self.engine
                    .clone()
                    .transaction(|tx| {
                        let keys = keys.clone();
                        async move {
                            for key in keys {
                                tx.delete(key.as_bytes()).await.map_err(|e| {
                                    TitoError::BackupFailed(format!(
                                        "Delete event changelog entry: {}",
                                        e
                                    ))
                                })?;
                            }
                            Ok::<_, TitoError>(())
                        }
                    })
                    .await?;
            }
        }

        Ok(total_entries)
    }

    pub async fn run_full_state_backup(&self) -> Result<usize, TitoError> {
        let timestamp = Utc::now().timestamp();
        let root = &self.config.backup_root;

        let mut cursor: Vec<u8> = b"table:".to_vec();
        let end_key: Vec<u8> = b"table:\xff".to_vec();
        let mut seq = 0;
        let mut total_entries = 0;
        let chunk_size = self.config.chunk_size;

        loop {
            let batch_cursor = cursor.clone();
            let batch_end = end_key.clone();
            let results: Vec<(Vec<u8>, Vec<u8>)> = self
                .engine
                .clone()
                .transaction(|tx| async move {
                    tx.scan(batch_cursor..batch_end, chunk_size)
                        .await
                        .map_err(|e| TitoError::BackupFailed(format!("Scan failed: {:?}", e)))
                })
                .await?;

            if results.is_empty() {
                break;
            }

            let last_key = results.last().map(|(k, _)| k.clone());
            let entry_count = results.len();

            let entries: Vec<ChangelogEntry> = results
                .into_iter()
                .filter_map(|(key_bytes, value_bytes)| {
                    let key = String::from_utf8(key_bytes).ok()?;
                    let data: Value = serde_json::from_slice(&value_bytes).ok()?;
                    Some(ChangelogEntry {
                        op: "put".to_string(),
                        key,
                        data: Some(data),
                    })
                })
                .collect();

            let chunk_key = format!("{}/state/full/{}/chunk-{:04}.json.gz", root, timestamp, seq);
            self.compress_and_upload_changelog(&entries, &chunk_key)
                .await?;

            total_entries += entry_count;
            seq += 1;

            if let Some(last) = last_key {
                cursor = last;
                cursor.push(0);
            } else {
                break;
            }
        }

        self.upload_marker(&format!("{}/LATEST_STATE_FULL", root), timestamp)
            .await?;

        let all_partial_keys = self
            .storage
            .list_keys(&format!("{}/state/partial/", root))
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List partial keys: {}", e)))?;

        if !all_partial_keys.is_empty() {
            self.storage
                .delete_keys(all_partial_keys)
                .await
                .map_err(|e| TitoError::BackupFailed(format!("Delete partial chunks: {}", e)))?;
        }

        let retention_cutoff = timestamp - self.config.retention_secs;
        let all_full_keys = self
            .storage
            .list_keys(&format!("{}/state/full/", root))
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List full backup keys: {}", e)))?;

        let old_full_keys: Vec<String> = all_full_keys
            .into_iter()
            .filter(|k| extract_backup_timestamp(k).map_or(false, |ts| ts < retention_cutoff))
            .collect();

        if !old_full_keys.is_empty() {
            self.storage
                .delete_keys(old_full_keys)
                .await
                .map_err(|e| TitoError::BackupFailed(format!("Delete old backups: {}", e)))?;
        }

        let old_event_keys = self
            .storage
            .list_keys(&format!("{}/events/", root))
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List event keys: {}", e)))?;

        let old_event_keys: Vec<String> = old_event_keys
            .into_iter()
            .filter(|k| extract_backup_timestamp(k).map_or(false, |ts| ts < retention_cutoff))
            .collect();

        if !old_event_keys.is_empty() {
            self.storage
                .delete_keys(old_event_keys)
                .await
                .map_err(|e| TitoError::BackupFailed(format!("Delete old event chunks: {}", e)))?;
        }

        Ok(total_entries)
    }

    pub async fn restore(
        &self,
        timestamp: Option<i64>,
        partition_count: u32,
    ) -> Result<RestoreResult, TitoError> {
        let root = &self.config.backup_root;

        self.engine
            .delete_range(&[], &[0xFFu8; 32])
            .await
            .map_err(|e| TitoError::BackupFailed(format!("Wipe failed: {:?}", e)))?;

        let full_backup_ts = self
            .find_full_backup_timestamp(timestamp)
            .await?
            .ok_or_else(|| TitoError::BackupFailed("No full backup found".to_string()))?;

        let full_keys = self
            .list_chunk_keys(&format!("{}/state/full/{}/", root, full_backup_ts))
            .await?;

        if full_keys.is_empty() {
            return Err(TitoError::BackupFailed(format!(
                "Full backup at {} has no chunk files",
                full_backup_ts
            )));
        }

        let mut state_entries_restored = self.replay_state_chunks(&full_keys).await?;

        let partial_keys = self.find_partial_keys(full_backup_ts, timestamp).await?;

        if !partial_keys.is_empty() {
            state_entries_restored += self.replay_state_chunks(&partial_keys).await?;
        }

        let event_keys = self.find_event_keys(timestamp).await?;
        let mut events_replayed = 0;

        if !event_keys.is_empty() {
            events_replayed = self
                .replay_event_chunks(&event_keys, partition_count)
                .await?;
        }

        Ok(RestoreResult {
            state_entries_restored,
            events_replayed,
        })
    }

    async fn check_and_run_full_state_backup(&self) -> Result<Option<usize>, TitoError> {
        let now = Utc::now().timestamp();
        let root = &self.config.backup_root;

        let last_full_ts = self
            .download_marker_timestamp(&format!("{}/LATEST_STATE_FULL", root))
            .await
            .unwrap_or(0);

        if now - last_full_ts < self.config.full_backup_interval_secs {
            return Ok(None);
        }

        let total = self.run_full_state_backup().await?;
        Ok(Some(total))
    }

    async fn compress_and_upload_changelog(
        &self,
        entries: &[ChangelogEntry],
        chunk_key: &str,
    ) -> Result<(), TitoError> {
        let json_bytes = serde_json::to_vec(entries)
            .map_err(|e| TitoError::BackupFailed(format!("Serialize: {}", e)))?;
        self.compress_and_upload_bytes(&json_bytes, chunk_key).await
    }

    async fn compress_and_upload_events(
        &self,
        entries: &[EventChangelogEntry],
        chunk_key: &str,
    ) -> Result<(), TitoError> {
        let json_bytes = serde_json::to_vec(entries)
            .map_err(|e| TitoError::BackupFailed(format!("Serialize: {}", e)))?;
        self.compress_and_upload_bytes(&json_bytes, chunk_key).await
    }

    async fn compress_and_upload_bytes(
        &self,
        json_bytes: &[u8],
        chunk_key: &str,
    ) -> Result<(), TitoError> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(json_bytes)
            .map_err(|e| TitoError::BackupFailed(format!("Compress: {}", e)))?;
        let compressed = encoder
            .finish()
            .map_err(|e| TitoError::BackupFailed(format!("Finish compress: {}", e)))?;

        self.storage
            .upload(chunk_key, &compressed)
            .await
            .map_err(|e| TitoError::BackupFailed(format!("Upload: {}", e)))?;

        Ok(())
    }

    async fn upload_marker(&self, key: &str, value: i64) -> Result<(), TitoError> {
        self.storage
            .upload(key, value.to_string().as_bytes())
            .await
            .map_err(|e| TitoError::BackupFailed(format!("Upload marker: {}", e)))
    }

    async fn download_marker_timestamp(&self, key: &str) -> Option<i64> {
        let bytes = self.storage.download(key).await.ok()?;
        let ts_str = String::from_utf8_lossy(&bytes);
        ts_str.trim().parse::<i64>().ok()
    }

    async fn find_full_backup_timestamp(
        &self,
        restore_ts: Option<i64>,
    ) -> Result<Option<i64>, TitoError> {
        let root = &self.config.backup_root;
        let all_keys = self
            .storage
            .list_keys(&format!("{}/state/full/", root))
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List full backups: {}", e)))?;

        if all_keys.is_empty() {
            return Ok(None);
        }

        let mut timestamps: Vec<i64> = all_keys
            .iter()
            .filter_map(|k| extract_backup_timestamp(k))
            .collect();
        timestamps.sort();
        timestamps.dedup();

        if timestamps.is_empty() {
            return Ok(None);
        }

        let selected = match restore_ts {
            Some(ts) => timestamps.into_iter().filter(|&t| t <= ts).last(),
            None => timestamps.into_iter().last(),
        };

        Ok(selected)
    }

    async fn find_partial_keys(
        &self,
        full_ts: i64,
        restore_ts: Option<i64>,
    ) -> Result<Vec<String>, TitoError> {
        let root = &self.config.backup_root;
        let all_keys = self
            .storage
            .list_keys(&format!("{}/state/partial/", root))
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List partial keys: {}", e)))?;

        let mut chunk_keys: Vec<String> = all_keys
            .into_iter()
            .filter(|k| k.ends_with(".json.gz"))
            .filter(|k| {
                extract_backup_timestamp(k).map_or(false, |ts| {
                    ts >= full_ts && restore_ts.map_or(true, |rts| ts <= rts)
                })
            })
            .collect();

        chunk_keys.sort();
        Ok(chunk_keys)
    }

    async fn find_event_keys(&self, restore_ts: Option<i64>) -> Result<Vec<String>, TitoError> {
        let root = &self.config.backup_root;
        let all_keys = self
            .storage
            .list_keys(&format!("{}/events/", root))
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List event keys: {}", e)))?;

        let mut chunk_keys: Vec<String> = all_keys
            .into_iter()
            .filter(|k| k.ends_with(".json.gz"))
            .filter(|k| {
                restore_ts.map_or(true, |rts| {
                    extract_backup_timestamp(k).map_or(false, |ts| ts <= rts)
                })
            })
            .collect();

        chunk_keys.sort();
        Ok(chunk_keys)
    }

    async fn list_chunk_keys(&self, prefix: &str) -> Result<Vec<String>, TitoError> {
        let all_keys = self
            .storage
            .list_keys(prefix)
            .await
            .map_err(|e| TitoError::BackupFailed(format!("List chunk keys: {}", e)))?;

        let mut chunk_keys: Vec<String> = all_keys
            .into_iter()
            .filter(|k| k.ends_with(".json.gz"))
            .collect();
        chunk_keys.sort();
        Ok(chunk_keys)
    }

    async fn replay_state_chunks(&self, chunk_keys: &[String]) -> Result<usize, TitoError> {
        let mut total_restored = 0;

        for chunk_key in chunk_keys {
            let compressed_bytes =
                self.storage.download(chunk_key).await.map_err(|e| {
                    TitoError::BackupFailed(format!("Download {}: {}", chunk_key, e))
                })?;

            let mut decoder = GzDecoder::new(&compressed_bytes[..]);
            let mut json_bytes = Vec::new();
            decoder
                .read_to_end(&mut json_bytes)
                .map_err(|e| TitoError::BackupFailed(format!("Decompress {}: {}", chunk_key, e)))?;

            let entries: Vec<ChangelogEntry> = serde_json::from_slice(&json_bytes)
                .map_err(|e| TitoError::BackupFailed(format!("Parse {}: {}", chunk_key, e)))?;

            let chunk_count = entries.len();

            for batch in entries.chunks(100) {
                let batch: Vec<ChangelogEntry> = batch.to_vec();
                self.engine
                    .clone()
                    .transaction(|tx| {
                        let batch = batch.clone();
                        async move {
                            for entry in batch {
                                match entry.op.as_str() {
                                    "put" => {
                                        if let Some(data) = entry.data {
                                            let bytes = serde_json::to_vec(&data).map_err(|e| {
                                                TitoError::BackupFailed(format!(
                                                    "Serialize {}: {}",
                                                    entry.key, e
                                                ))
                                            })?;
                                            tx.put(entry.key.as_bytes(), bytes).await.map_err(
                                                |e| {
                                                    TitoError::BackupFailed(format!(
                                                        "Put {}: {}",
                                                        entry.key, e
                                                    ))
                                                },
                                            )?;
                                        }
                                    }
                                    "delete" => {
                                        tx.delete(entry.key.as_bytes()).await.map_err(|e| {
                                            TitoError::BackupFailed(format!(
                                                "Delete {}: {}",
                                                entry.key, e
                                            ))
                                        })?;
                                    }
                                    _ => {}
                                }
                            }
                            Ok::<_, TitoError>(())
                        }
                    })
                    .await?;
            }

            total_restored += chunk_count;
        }

        Ok(total_restored)
    }

    async fn replay_event_chunks(
        &self,
        chunk_keys: &[String],
        partition_count: u32,
    ) -> Result<usize, TitoError> {
        let mut total_replayed = 0;

        for chunk_key in chunk_keys {
            let compressed_bytes =
                self.storage.download(chunk_key).await.map_err(|e| {
                    TitoError::BackupFailed(format!("Download {}: {}", chunk_key, e))
                })?;

            let mut decoder = GzDecoder::new(&compressed_bytes[..]);
            let mut json_bytes = Vec::new();
            decoder
                .read_to_end(&mut json_bytes)
                .map_err(|e| TitoError::BackupFailed(format!("Decompress {}: {}", chunk_key, e)))?;

            let entries: Vec<EventChangelogEntry> = serde_json::from_slice(&json_bytes)
                .map_err(|e| TitoError::BackupFailed(format!("Parse {}: {}", chunk_key, e)))?;

            let chunk_count = entries.len();

            for batch in entries.chunks(100) {
                let batch: Vec<EventChangelogEntry> = batch.to_vec();
                let pc = partition_count;
                self.engine
                    .clone()
                    .transaction(|tx| {
                        let batch = batch.clone();
                        async move {
                            for entry in batch {
                                let mut hasher = DefaultHasher::new();
                                entry.event_key.hash(&mut hasher);
                                let partition = (hasher.finish() % pc as u64) as u32;

                                let queue_key = format!(
                                    "queue:{:0pwidth$}:{}:{}",
                                    partition,
                                    entry.timestamp,
                                    entry.event_id,
                                    pwidth = PARTITION_DIGITS,
                                );

                                let queue_value = serde_json::to_vec(&entry.data).map_err(|e| {
                                    TitoError::BackupFailed(format!(
                                        "Serialize event {}: {}",
                                        entry.event_id, e
                                    ))
                                })?;

                                tx.put(queue_key.as_bytes(), queue_value)
                                    .await
                                    .map_err(|e| {
                                        TitoError::BackupFailed(format!(
                                            "Put event {}: {}",
                                            entry.event_id, e
                                        ))
                                    })?;
                            }
                            Ok::<_, TitoError>(())
                        }
                    })
                    .await?;
            }

            total_replayed += chunk_count;
        }

        Ok(total_replayed)
    }
}

async fn scan_changelog_range<T: TitoTransaction>(
    start_key: &str,
    prefix: &str,
    limit: u32,
    tx: &T,
) -> Result<(Vec<(String, ChangelogEntry)>, Option<String>), TitoError> {
    let end_key = next_string_lexicographically(prefix.to_string());
    let results = tx
        .scan(start_key.as_bytes()..end_key.as_bytes(), limit)
        .await
        .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

    let mut entries = Vec::new();
    let mut last_key = None;

    for (key_bytes, value_bytes) in results {
        let key = String::from_utf8(key_bytes)
            .map_err(|_| TitoError::DeserializationFailed("Invalid key".to_string()))?;
        let entry: ChangelogEntry = serde_json::from_slice(&value_bytes)
            .map_err(|_| TitoError::DeserializationFailed("Invalid changelog entry".to_string()))?;
        last_key = Some(key.clone());
        entries.push((key, entry));
    }

    Ok((entries, last_key))
}

async fn scan_event_changelog_range<T: TitoTransaction>(
    start_key: &str,
    limit: u32,
    tx: &T,
) -> Result<(Vec<(String, EventChangelogEntry)>, Option<String>), TitoError> {
    let end_key = next_string_lexicographically("event_changelog:".to_string());
    let results = tx
        .scan(start_key.as_bytes()..end_key.as_bytes(), limit)
        .await
        .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

    let mut entries = Vec::new();
    let mut last_key = None;

    for (key_bytes, value_bytes) in results {
        let key = String::from_utf8(key_bytes)
            .map_err(|_| TitoError::DeserializationFailed("Invalid key".to_string()))?;
        let entry: EventChangelogEntry = serde_json::from_slice(&value_bytes).map_err(|_| {
            TitoError::DeserializationFailed("Invalid event changelog entry".to_string())
        })?;
        last_key = Some(key.clone());
        entries.push((key, entry));
    }

    Ok((entries, last_key))
}

fn extract_backup_timestamp(key: &str) -> Option<i64> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() >= 4 {
        parts[parts.len() - 2].parse::<i64>().ok()
    } else {
        None
    }
}
