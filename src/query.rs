use crate::{
    error::TitoError,
    types::{TitoEngine, TitoFindByIndexPayload, TitoModelTrait, TitoPaginated},
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};

pub struct IndexQueryBuilder<E, T>
where
    E: TitoEngine,
    T: Default
        + Clone
        + Serialize
        + DeserializeOwned
        + Unpin
        + std::marker::Send
        + Sync
        + TitoModelTrait,
{
    model: TitoModel<E, T>,
    index: String,
    values: Vec<String>,
    rels: Vec<String>,
    end: Option<String>,
    exact_match: bool,
    limit: Option<u32>,
    cursor: Option<String>,
}

impl<E, T> IndexQueryBuilder<E, T>
where
    E: TitoEngine,
    T: Clone
        + Serialize
        + DeserializeOwned
        + Unpin
        + std::marker::Send
        + Sync
        + Default
        + TitoModelTrait,
{
    pub fn new(model: TitoModel<E, T>, index: String) -> Self {
        Self {
            model: model.clone(),
            index,
            values: Vec::new(),
            rels: Vec::new(),
            end: None,
            exact_match: true,
            limit: None,
            cursor: None,
        }
    }

    // Modified to use &mut self
    pub fn value(&mut self, value: impl Into<String>) -> &mut Self {
        self.values.push(value.into());
        self
    }

    // Modified to use &mut self
    pub fn exact_match(&mut self, value: bool) -> &mut Self {
        self.exact_match = value;
        self
    }

    // Modified to use &mut self
    pub fn relationship(&mut self, rel: impl Into<String>) -> &mut Self {
        self.rels.push(rel.into());
        self
    }

    // Modified to use &mut self
    pub fn limit(&mut self, limit: Option<u32>) -> &mut Self {
        self.limit = limit;
        self
    }

    // Modified to use &mut self
    pub fn cursor(&mut self, cursor: Option<impl Into<String>>) -> &mut Self {
        self.cursor = cursor.map(|cursor| cursor.into());
        self
    }

    // Modified to use &mut self
    pub fn end(&mut self, end: Option<impl Into<String>>) -> &mut Self {
        self.end = end.map(|end| end.into());
        self
    }

    // Terminal method with optional transaction
    pub async fn execute(
        &mut self,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let payload = TitoFindByIndexPayload {
            index: self.index.clone(),
            values: self.values.clone(),
            rels: self.rels.clone(),
            end: self.end.clone(),
            exact_match: self.exact_match,
            limit: self.limit,
            cursor: self.cursor.clone(),
        };

        self.model.find_by_index(payload, tx).await
    }

    // Terminal method with optional transaction
    pub async fn execute_reverse(
        &mut self,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let payload = TitoFindByIndexPayload {
            index: self.index.clone(),
            values: self.values.clone(),
            rels: self.rels.clone(),
            end: self.end.clone(),
            exact_match: self.exact_match,
            limit: self.limit,
            cursor: self.cursor.clone(),
        };

        self.model.find_by_index_reverse(payload, tx).await
    }
}
