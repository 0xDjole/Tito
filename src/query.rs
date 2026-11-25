use crate::{
    backend::tikv::{TiKVBackend, TiKVTransaction},
    base::{check_cursor, decode_cursor, encode_cursor, noted, utf8_key},
    error::TitoError,
    index,
    read::{scan_rows, Found, Noted, Read, Request, ScanRequest},
    types::{
        Guard, TitoEngine, TitoIndexBound, TitoIndexField, TitoModelConstraints, TitoPaginated,
    },
    utils::key_after_bytes,
    TitoModel,
};
use serde::de::DeserializeOwned;

pub struct IndexQueryBuilder<E: TitoEngine, T> {
    model: TitoModel<E, T>,
    index: String,
    values: Vec<TitoIndexField>,
    lower: Option<TitoIndexBound>,
    upper: Option<TitoIndexBound>,
    descending: bool,
    limit: Option<u32>,
    cursor: Option<String>,
}

impl<E: TitoEngine, T: TitoModelConstraints> IndexQueryBuilder<E, T> {
    pub(crate) fn new(model: TitoModel<E, T>, index: String) -> Self {
        Self {
            model,
            index,
            values: Vec::new(),
            lower: None,
            upper: None,
            descending: false,
            limit: None,
            cursor: None,
        }
    }

    pub fn value(&mut self, value: TitoIndexField) -> &mut Self {
        self.values.push(value);
        self
    }

    pub fn lower(&mut self, bound: TitoIndexBound) -> &mut Self {
        self.lower = Some(bound);
        self
    }

    pub fn upper(&mut self, bound: TitoIndexBound) -> &mut Self {
        self.upper = Some(bound);
        self
    }

    pub fn descending(&mut self) -> &mut Self {
        self.descending = true;
        self
    }

    pub fn limit(&mut self, limit: Option<u32>) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn cursor(&mut self, cursor: Option<impl Into<String>>) -> &mut Self {
        self.cursor = cursor.map(Into::into);
        self
    }

    fn scan(&self) -> Result<(ScanRequest, u32), TitoError> {
        let limit = match self.limit {
            Some(0) => {
                return Err(TitoError::InvalidInput(
                    "Index query limit must be greater than zero".to_string(),
                ))
            }
            Some(limit) => limit,
            None => u32::MAX,
        };
        let (start, end) = index::query_range(
            &self.index,
            &self.values,
            self.lower.as_ref(),
            self.upper.as_ref(),
        )?;
        let mut start = start.into_bytes();
        let mut end = end.into_bytes();
        if let Some(cursor) = &self.cursor {
            let last = decode_cursor(cursor)?;
            check_cursor(&last, &start, &end)?;
            if self.descending {
                end = last;
            } else {
                start = key_after_bytes(&last);
            }
        }
        let request = ScanRequest {
            start,
            end,
            limit: limit.saturating_add(1),
            reverse: self.descending,
        };
        Ok((request, limit))
    }

    fn page<P: Send + 'static>(
        &self,
        decode: impl Fn(&str, &[u8]) -> Result<(P, Option<Noted>), TitoError> + Send + Sync + 'static,
    ) -> Read<TitoPaginated<P>> {
        let (request, limit) = match self.scan() {
            Ok(scan) => scan,
            Err(error) => return Read::refused(error),
        };
        Read::new(Request::Scan(request), Guard::None, move |rows| {
            let mut rows = scan_rows(rows)?;
            let more = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let cursor = match rows.last() {
                Some((key, _)) if more => Some(encode_cursor(key)?),
                _ => None,
            };
            let mut found = Found {
                value: TitoPaginated::new(Vec::with_capacity(rows.len()), cursor),
                records: Vec::new(),
            };
            for (key, value) in rows {
                let key = utf8_key(key)?;
                let (item, record) = decode(key.as_str(), value.as_slice())?;
                found.value.items.push(item);
                found.records.extend(record);
            }
            Ok(found)
        })
    }

    pub fn read(&self) -> Read<TitoPaginated<T>> {
        self.page(|key, value| {
            let record = index::full_copy::<T>(key, value)?;
            let record_note = noted(&record)?;
            Ok((record, Some(record_note)))
        })
    }

    pub fn read_part<P: DeserializeOwned + Send + 'static>(&self) -> Read<TitoPaginated<P>> {
        self.page(|key, value| Ok((index::part_copy::<P>(key, value)?, None)))
    }

    pub fn read_ids(&self) -> Read<TitoPaginated<String>> {
        self.page(|key, value| Ok((index::entry_id::<T>(key, value)?, None)))
    }
}

impl<T: TitoModelConstraints> IndexQueryBuilder<TiKVBackend, T> {
    pub async fn execute(
        &mut self,
        tx: Option<&TiKVTransaction>,
    ) -> Result<TitoPaginated<T>, TitoError> {
        self.run(self.read(), tx).await
    }

    pub async fn execute_part<P: DeserializeOwned + Send + 'static>(
        &mut self,
        tx: Option<&TiKVTransaction>,
    ) -> Result<TitoPaginated<P>, TitoError> {
        self.run(self.read_part::<P>(), tx).await
    }

    pub async fn execute_ids(
        &mut self,
        tx: Option<&TiKVTransaction>,
    ) -> Result<TitoPaginated<String>, TitoError> {
        self.run(self.read_ids(), tx).await
    }

    async fn run<P: Send + 'static>(
        &self,
        read: Read<P>,
        tx: Option<&TiKVTransaction>,
    ) -> Result<P, TitoError> {
        match tx {
            Some(tx) => tx.batch_get(read).await,
            None => {
                self.model
                    .engine
                    .transaction(|tx| async move { tx.batch_get(read).await })
                    .await
            }
        }
    }
}
