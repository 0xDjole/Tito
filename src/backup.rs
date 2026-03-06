use crate::{error::TitoError, types::TitoTransaction, utils::next_string_lexicographically};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChangelogEntry {
    pub op: String,
    pub key: String,
    pub data: Option<Value>,
}

#[derive(Clone)]
pub struct TitoBackupService;

impl TitoBackupService {
    pub fn new() -> Self {
        Self
    }

    pub async fn scan_changelog<T: TitoTransaction>(
        &self,
        start_key: &str,
        limit: u32,
        tx: &T,
    ) -> Result<(Vec<(String, ChangelogEntry)>, Option<String>), TitoError> {
        let end_key = next_string_lexicographically("changelog:".to_string());
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
                .map_err(|_| {
                    TitoError::DeserializationFailed("Invalid changelog entry".to_string())
                })?;
            last_key = Some(key.clone());
            entries.push((key, entry));
        }

        Ok((entries, last_key))
    }

    pub async fn delete_changelog_range<T: TitoTransaction>(
        &self,
        keys: Vec<String>,
        tx: &T,
    ) -> Result<usize, TitoError> {
        let count = keys.len();
        for key in keys {
            tx.delete(key.as_bytes())
                .await
                .map_err(|e| TitoError::DeleteFailed(format!("Delete failed: {}", e)))?;
        }
        Ok(count)
    }
}
