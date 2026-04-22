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
    limit: Option<u32>,
    cursor: Option<String>,
}

impl<E, T> IndexQueryBuilder<E, T>
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
    pub fn new(model: TitoModel<E, T>, index: String) -> Self {
        Self {
            model: model.clone(),
            index,
            values: Vec::new(),
            rels: Vec::new(),
            limit: None,
            cursor: None,
        }
    }

    pub fn value(&mut self, value: impl Into<String>) -> &mut Self {
        self.values.push(value.into());
        self
    }

    pub fn relationship(&mut self, rel: impl Into<String>) -> &mut Self {
        self.rels.push(rel.into());
        self
    }

    pub fn limit(&mut self, limit: Option<u32>) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn cursor(&mut self, cursor: Option<impl Into<String>>) -> &mut Self {
        self.cursor = cursor.map(|cursor| cursor.into());
        self
    }

    pub async fn execute(
        &mut self,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let payload = TitoFindByIndexPayload {
            index: self.index.clone(),
            values: self.values.clone(),
            rels: self.rels.clone(),
            limit: self.limit,
            cursor: self.cursor.clone(),
        };

        self.model.find_by_index(payload, tx).await
    }

    pub async fn execute_reverse(
        &mut self,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let payload = TitoFindByIndexPayload {
            index: self.index.clone(),
            values: self.values.clone(),
            rels: self.rels.clone(),
            limit: self.limit,
            cursor: self.cursor.clone(),
        };

        self.model.find_by_index_reverse(payload, tx).await
    }
}
