use std::net::SocketAddr;
use tito::backend::tikv::{TiKV, TiKVBackend};
use tito::types::TitoModelConstraints;
use tito::{Guard, TitoEngine, TitoError, TitoModel};

pub fn native_endpoint() -> (String, String) {
    let endpoint = std::env::var("TITO_NATIVE_PD_URI").expect(
        "TITO_NATIVE_PD_URI must name the disposable PD endpoint owned by the verification run",
    );
    let address: SocketAddr = endpoint
        .parse()
        .expect("The owned PD endpoint must be an explicit loopback host:port");
    assert!(
        address.ip().is_loopback() && address.port() != 0,
        "The native case requires its declared disposable loopback endpoint"
    );
    let run_id = std::env::var("TITO_NATIVE_RUN_ID")
        .expect("TITO_NATIVE_RUN_ID must name the disposable verification run");
    assert!(
        run_id.starts_with("run-")
            && run_id.len() > 4
            && run_id.len() <= 64
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "The native case requires an explicit bounded disposable run identity"
    );
    (endpoint, run_id)
}

pub async fn native_engine(endpoint: String) -> TiKVBackend {
    TiKV::connect(vec![endpoint])
        .await
        .expect("Connect to the declared test-owned native PD and TiKV")
}

pub async fn create<T: TitoModelConstraints>(
    engine: &TiKVBackend,
    model: &TitoModel<TiKVBackend, T>,
    record: &T,
) -> Result<(), TitoError> {
    engine
        .transaction(|tx| {
            let model = model.clone();
            let record = record.clone();
            async move { model.create(&record, &tx).await }
        })
        .await
}

pub async fn read<T: TitoModelConstraints>(
    engine: &TiKVBackend,
    model: &TitoModel<TiKVBackend, T>,
    id: &str,
) -> Result<Option<T>, TitoError> {
    let note = model.read_maybe(id, Guard::None);
    engine
        .transaction(|tx| async move { tx.batch_get(note).await })
        .await
}

pub async fn remove<T: TitoModelConstraints>(
    engine: &TiKVBackend,
    model: &TitoModel<TiKVBackend, T>,
    id: &str,
) -> Result<(), TitoError> {
    let note = model.read(id, Guard::None);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let note = note.clone();
            async move {
                let record = tx.batch_get(note).await?;
                model.remove(&record, &tx).await
            }
        })
        .await
}
