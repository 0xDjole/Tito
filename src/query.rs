use crate::{
    transaction::TitoTransaction,
    types::{TitoError, TitoFindByIndexPayload, TitoModelTrait, TitoPaginated},
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};

pub struct IndexQueryBuilder<T>
where
    T: Default
        + Clone
        + Serialize
        + DeserializeOwned
        + Unpin
        + std::marker::Send
        + Sync
        + TitoModelTrait,
{
    model: TitoModel<T>,
    index: String,
    values: Vec<String>,
    rels: Vec<String>,
    end: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
}

impl<T> IndexQueryBuilder<T>
where
    T: Clone
        + Serialize
        + DeserializeOwned
        + Unpin
        + std::marker::Send
        + Sync
        + Default
        + TitoModelTrait,
{
    pub fn new(model: TitoModel<T>, index: String) -> Self {
        Self {
            model: model.clone(),
            index,
            values: Vec::new(),
            rels: Vec::new(),
            end: None,
            limit: None,
            cursor: None,
        }
    }

    // Add a vector of values
    pub fn values(mut self, values: Vec<String>) -> Self {
        self.values = values;
        self
    }

    // Add a single value
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }

    // Set relationships to include
    pub fn relationships(mut self, rels: Vec<String>) -> Self {
        self.rels = rels;
        self
    }

    // Add a single relationship
    pub fn relationship(mut self, rel: impl Into<String>) -> Self {
        self.rels.push(rel.into());
        self
    }

    // Set result limit
    pub fn limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    // Set pagination cursor
    pub fn cursor(mut self, cursor: Option<impl Into<String>>) -> Self {
        self.cursor = cursor.map(|cursor| cursor.into());
        self
    }

    // Set end key
    pub fn end(mut self, end: Option<impl Into<String>>) -> Self {
        self.end = end.map(|end| end.into());
        self
    }

    // Execute with internal transaction
    pub async fn execute(self) -> Result<TitoPaginated<T>, TitoError> {
        let payload = TitoFindByIndexPayload {
            index: self.index,
            values: self.values,
            rels: self.rels,
            end: self.end,
            limit: self.limit,
            cursor: self.cursor,
        };

        self.model.find_by_index(payload).await
    }

    // Execute with provided transaction
    pub async fn execute_tx(self, tx: &TitoTransaction) -> Result<TitoPaginated<T>, TitoError> {
        let payload = TitoFindByIndexPayload {
            index: self.index,
            values: self.values,
            rels: self.rels,
            end: self.end,
            limit: self.limit,
            cursor: self.cursor,
        };

        self.model.find_by_index_tx(payload, tx).await
    }
}
