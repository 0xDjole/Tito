use crate::backend::tikv::TiKVTransaction;
use crate::error::TitoError;
use crate::index::RememberedEntry;
use crate::types::{Guard, TitoKvPair};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ScanRequest {
    pub(crate) start: Vec<u8>,
    pub(crate) end: Vec<u8>,
    pub(crate) limit: u32,
    pub(crate) reverse: bool,
}

#[derive(Clone)]
pub(crate) enum Request {
    Keys(Vec<Vec<u8>>),
    Scan(ScanRequest),
}

pub(crate) enum Rows {
    Keys(Vec<Option<Vec<u8>>>),
    Scan(Vec<TitoKvPair>),
}

pub(crate) struct Noted {
    pub(crate) table: String,
    pub(crate) id: String,
    pub(crate) key: String,
    pub(crate) entries: Vec<RememberedEntry>,
}

pub(crate) struct Found<T> {
    pub(crate) value: T,
    pub(crate) records: Vec<Noted>,
}

type Answer<T> = Arc<dyn Fn(Rows) -> Result<Found<T>, TitoError> + Send + Sync>;

pub struct Read<T> {
    request: Request,
    guard: Guard,
    answer: Answer<T>,
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            guard: self.guard,
            answer: self.answer.clone(),
        }
    }
}

impl<T: Send + 'static> Read<T> {
    pub(crate) fn new(
        request: Request,
        guard: Guard,
        answer: impl Fn(Rows) -> Result<Found<T>, TitoError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            request,
            guard,
            answer: Arc::new(answer),
        }
    }

    pub(crate) fn refused(error: TitoError) -> Self {
        Self::new(Request::Keys(Vec::new()), Guard::None, move |_| {
            Err(error.clone())
        })
    }

    pub fn map<U: Send + 'static>(
        self,
        convert: impl Fn(T) -> Result<U, TitoError> + Send + Sync + 'static,
    ) -> Read<U> {
        let answer = self.answer;
        Read::new(self.request, self.guard, move |rows| {
            let found = answer(rows)?;
            Ok(Found {
                value: convert(found.value)?,
                records: found.records,
            })
        })
    }
}

pub(crate) fn single_row(rows: Rows) -> Result<Option<Vec<u8>>, TitoError> {
    match rows {
        Rows::Keys(mut values) if values.len() == 1 => Ok(values.pop().flatten()),
        _ => Err(TitoError::Internal(
            "A read by key was answered with the rows of another request".to_string(),
        )),
    }
}

pub(crate) fn key_rows(rows: Rows) -> Result<Vec<Option<Vec<u8>>>, TitoError> {
    match rows {
        Rows::Keys(values) => Ok(values),
        Rows::Scan(_) => Err(TitoError::Internal(
            "A read by key was answered with the rows of a scan".to_string(),
        )),
    }
}

pub(crate) fn scan_rows(rows: Rows) -> Result<Vec<TitoKvPair>, TitoError> {
    match rows {
        Rows::Scan(pairs) => Ok(pairs),
        Rows::Keys(_) => Err(TitoError::Internal(
            "A scan was answered with the rows of a read by key".to_string(),
        )),
    }
}

#[derive(Default)]
pub struct BatchRequests {
    requests: Vec<Request>,
}

impl BatchRequests {
    pub(crate) fn push(&mut self, request: Request) {
        self.requests.push(request);
    }
}

pub struct BatchAnswers {
    rows: VecDeque<Rows>,
}

impl BatchAnswers {
    pub(crate) fn next(&mut self) -> Result<Rows, TitoError> {
        self.rows.pop_front().ok_or_else(|| {
            TitoError::Internal("A batch element found no answer of its own".to_string())
        })
    }
}

#[async_trait]
pub trait Batch: Send {
    type Output: Send;
    type Error: From<TitoError> + Send;

    fn requests(&self, requests: &mut BatchRequests);

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error>;
}

impl TiKVTransaction {
    pub async fn batch_get<B: Batch>(&self, batch: B) -> Result<B::Output, B::Error> {
        let mut requests = BatchRequests::default();
        batch.requests(&mut requests);
        let rows = self.fetch(requests.requests).await?;
        let mut answers = BatchAnswers { rows: rows.into() };
        batch.answer(&mut answers, self).await
    }
}

#[async_trait]
impl<T: Send + 'static> Batch for Read<T> {
    type Output = T;
    type Error = TitoError;

    fn requests(&self, requests: &mut BatchRequests) {
        requests.push(self.request.clone());
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<T, TitoError> {
        let rows = answers.next()?;
        let found = (self.answer)(rows)?;
        tx.remember(found.records, self.guard).await;
        Ok(found.value)
    }
}

#[async_trait]
impl<B: Batch> Batch for Vec<B> {
    type Output = Vec<B::Output>;
    type Error = B::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        for batch in self {
            batch.requests(requests);
        }
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let mut outputs = Vec::with_capacity(self.len());
        for batch in self {
            outputs.push(batch.answer(answers, tx).await?);
        }
        Ok(outputs)
    }
}

#[async_trait]
impl<A, B> Batch for (A, B)
where
    A: Batch,
    B: Batch,
    B::Error: Into<A::Error>,
{
    type Output = (A::Output, B::Output);
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b))
    }
}

#[async_trait]
impl<A, B, C> Batch for (A, B, C)
where
    A: Batch,
    B: Batch,
    C: Batch,
    B::Error: Into<A::Error>,
    C::Error: Into<A::Error>,
{
    type Output = (A::Output, B::Output, C::Output);
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
        self.2.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b, c) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let c = c
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b, c))
    }
}

#[async_trait]
impl<A, B, C, D> Batch for (A, B, C, D)
where
    A: Batch,
    B: Batch,
    C: Batch,
    D: Batch,
    B::Error: Into<A::Error>,
    C::Error: Into<A::Error>,
    D::Error: Into<A::Error>,
{
    type Output = (A::Output, B::Output, C::Output, D::Output);
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
        self.2.requests(requests);
        self.3.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b, c, d) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let c = c
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let d = d
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b, c, d))
    }
}

#[async_trait]
impl<A, B, C, D, E> Batch for (A, B, C, D, E)
where
    A: Batch,
    B: Batch,
    C: Batch,
    D: Batch,
    E: Batch,
    B::Error: Into<A::Error>,
    C::Error: Into<A::Error>,
    D::Error: Into<A::Error>,
    E::Error: Into<A::Error>,
{
    type Output = (A::Output, B::Output, C::Output, D::Output, E::Output);
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
        self.2.requests(requests);
        self.3.requests(requests);
        self.4.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b, c, d, e) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let c = c
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let d = d
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let e = e
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b, c, d, e))
    }
}

#[async_trait]
impl<A, B, C, D, E, F> Batch for (A, B, C, D, E, F)
where
    A: Batch,
    B: Batch,
    C: Batch,
    D: Batch,
    E: Batch,
    F: Batch,
    B::Error: Into<A::Error>,
    C::Error: Into<A::Error>,
    D::Error: Into<A::Error>,
    E::Error: Into<A::Error>,
    F::Error: Into<A::Error>,
{
    type Output = (
        A::Output,
        B::Output,
        C::Output,
        D::Output,
        E::Output,
        F::Output,
    );
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
        self.2.requests(requests);
        self.3.requests(requests);
        self.4.requests(requests);
        self.5.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b, c, d, e, f) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let c = c
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let d = d
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let e = e
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let f = f
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b, c, d, e, f))
    }
}

#[async_trait]
impl<A, B, C, D, E, F, G> Batch for (A, B, C, D, E, F, G)
where
    A: Batch,
    B: Batch,
    C: Batch,
    D: Batch,
    E: Batch,
    F: Batch,
    G: Batch,
    B::Error: Into<A::Error>,
    C::Error: Into<A::Error>,
    D::Error: Into<A::Error>,
    E::Error: Into<A::Error>,
    F::Error: Into<A::Error>,
    G::Error: Into<A::Error>,
{
    type Output = (
        A::Output,
        B::Output,
        C::Output,
        D::Output,
        E::Output,
        F::Output,
        G::Output,
    );
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
        self.2.requests(requests);
        self.3.requests(requests);
        self.4.requests(requests);
        self.5.requests(requests);
        self.6.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b, c, d, e, f, g) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let c = c
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let d = d
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let e = e
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let f = f
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let g = g
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b, c, d, e, f, g))
    }
}

#[async_trait]
impl<A, B, C, D, E, F, G, H> Batch for (A, B, C, D, E, F, G, H)
where
    A: Batch,
    B: Batch,
    C: Batch,
    D: Batch,
    E: Batch,
    F: Batch,
    G: Batch,
    H: Batch,
    B::Error: Into<A::Error>,
    C::Error: Into<A::Error>,
    D::Error: Into<A::Error>,
    E::Error: Into<A::Error>,
    F::Error: Into<A::Error>,
    G::Error: Into<A::Error>,
    H::Error: Into<A::Error>,
{
    type Output = (
        A::Output,
        B::Output,
        C::Output,
        D::Output,
        E::Output,
        F::Output,
        G::Output,
        H::Output,
    );
    type Error = A::Error;

    fn requests(&self, requests: &mut BatchRequests) {
        self.0.requests(requests);
        self.1.requests(requests);
        self.2.requests(requests);
        self.3.requests(requests);
        self.4.requests(requests);
        self.5.requests(requests);
        self.6.requests(requests);
        self.7.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Self::Output, Self::Error> {
        let (a, b, c, d, e, f, g, h) = self;
        let a = a.answer(answers, tx).await?;
        let b = b
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let c = c
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let d = d
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let e = e
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let f = f
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let g = g
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        let h = h
            .answer(answers, tx)
            .await
            .map_err(Into::<A::Error>::into)?;
        Ok((a, b, c, d, e, f, g, h))
    }
}
