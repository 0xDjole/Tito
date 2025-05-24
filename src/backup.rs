use crate::{error::TitoError, transaction::TitoTransaction, utils::next_string_lexicographically};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Serialize, Deserialize, Debug)]
pub struct BackupRecord {
    pub key: String,
    pub value: Value,
}

pub struct TitoBackupService;

impl TitoBackupService {
    pub fn new() -> Self {
        Self
    }

    pub async fn backup(&self, file_path: &str, tx: &TitoTransaction) -> Result<usize, TitoError> {
        if let Some(parent) = Path::new(file_path).parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| TitoError::Internal(format!("Failed to create directories: {}", e)))?;
        }

        let mut file = fs::File::create(file_path)
            .await
            .map_err(|e| TitoError::Internal(format!("Failed to create file: {}", e)))?;

        file.write_all(b"[\n")
            .await
            .map_err(|e| TitoError::Internal(format!("Failed to write: {}", e)))?;

        let mut record_count = 0;
        let mut first_record = true;

        record_count += self
            .write_prefix_to_file("table:", &mut file, &mut first_record, tx)
            .await?;

        file.write_all(b"\n]")
            .await
            .map_err(|e| TitoError::Internal(format!("Failed to write: {}", e)))?;

        file.flush()
            .await
            .map_err(|e| TitoError::Internal(format!("Failed to flush: {}", e)))?;

        Ok(record_count)
    }

    async fn write_prefix_to_file(
        &self,
        prefix: &str,
        file: &mut fs::File,
        first_record: &mut bool,
        tx: &TitoTransaction,
    ) -> Result<usize, TitoError> {
        let start_key = prefix.to_string();
        let end_key = next_string_lexicographically(start_key.clone());

        let mut cursor = start_key;
        let mut count = 0;

        loop {
            let scan_result = tx.scan(cursor.clone()..end_key.clone(), 1000).await?;
            let mut has_records = false;

            for kv in scan_result {
                has_records = true;
                let key = String::from_utf8(kv.0.into())
                    .map_err(|_| TitoError::DeserializationFailed("Invalid key".to_string()))?;

                let value: Value = serde_json::from_slice(&kv.1)
                    .map_err(|_| TitoError::DeserializationFailed("Invalid value".to_string()))?;

                let record = BackupRecord {
                    key: key.clone(),
                    value,
                };

                // Add comma before record if not first
                if !*first_record {
                    file.write_all(b",\n")
                        .await
                        .map_err(|e| TitoError::Internal(format!("Failed to write: {}", e)))?;
                }
                *first_record = false;

                // Write the record
                let record_json = serde_json::to_string_pretty(&record)
                    .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                file.write_all(record_json.as_bytes())
                    .await
                    .map_err(|e| TitoError::Internal(format!("Failed to write: {}", e)))?;

                count += 1;
                cursor = next_string_lexicographically(key);
            }

            if !has_records {
                break;
            }
        }

        Ok(count)
    }

    pub async fn delete_all_data(&self, tx: &TitoTransaction) -> Result<usize, TitoError> {
        let start_key = String::new();
        let end_key = String::from_utf8(vec![255; 10]).unwrap(); // Max key

        let mut deleted_count = 0;

        loop {
            let scan_result = tx.scan(start_key.clone()..end_key.clone(), 1000).await?;
            let mut keys_to_delete = Vec::new();

            for kv in scan_result {
                let key = String::from_utf8(kv.0.into())
                    .map_err(|_| TitoError::DeserializationFailed("Invalid key".to_string()))?;
                keys_to_delete.push(key);
            }

            if keys_to_delete.is_empty() {
                break;
            }

            // Delete in batches
            for key in &keys_to_delete {
                tx.delete(key.clone()).await?;
                deleted_count += 1;
            }
        }

        Ok(deleted_count)
    }

    async fn backup_prefix(
        &self,
        prefix: &str,
        tx: &TitoTransaction,
    ) -> Result<Vec<BackupRecord>, TitoError> {
        let start_key = prefix.to_string();
        let end_key = next_string_lexicographically(start_key.clone());

        let mut records = Vec::new();
        let mut cursor = start_key;

        loop {
            let scan_result = tx.scan(cursor.clone()..end_key.clone(), 1000).await?;
            let mut has_records = false;

            for kv in scan_result {
                has_records = true;
                let key = String::from_utf8(kv.0.into())
                    .map_err(|_| TitoError::DeserializationFailed("Invalid key".to_string()))?;

                let value: Value = serde_json::from_slice(&kv.1)
                    .map_err(|_| TitoError::DeserializationFailed("Invalid value".to_string()))?;

                records.push(BackupRecord {
                    key: key.clone(),
                    value,
                });
                cursor = next_string_lexicographically(key);
            }

            if !has_records {
                break;
            }
        }

        Ok(records)
    }

    // Delete specific prefix
    pub async fn delete_prefix(
        &self,
        prefix: &str,
        tx: &TitoTransaction,
    ) -> Result<usize, TitoError> {
        let start_key = prefix.to_string();
        let end_key = next_string_lexicographically(start_key.clone());

        let mut cursor = start_key;
        let mut deleted_count = 0;

        loop {
            let scan_result = tx.scan(cursor.clone()..end_key.clone(), 1000).await?;
            let mut keys_to_delete = Vec::new();

            for kv in scan_result {
                let key = String::from_utf8(kv.0.into())
                    .map_err(|_| TitoError::DeserializationFailed("Invalid key".to_string()))?;
                keys_to_delete.push(key.clone());
                cursor = next_string_lexicographically(key);
            }

            if keys_to_delete.is_empty() {
                break;
            }

            // Delete batch
            for key in &keys_to_delete {
                tx.delete(key.clone()).await?;
                deleted_count += 1;
            }
        }

        Ok(deleted_count)
    }

    pub async fn restore_events_and_rebuild(
        &self,
        tx: &TitoTransaction,
    ) -> Result<usize, TitoError> {
        let events = self.backup_prefix("event:", tx).await?;
        Ok(events.len())
    }
}
