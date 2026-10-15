use std::net::SocketAddr;
use tito::backend::tikv::{TiKV, TiKVBackend};
use tito::types::TitoModelConstraints;
use tito::{TitoEngine, TitoError, TitoModel};

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

pub async fn save<T: TitoModelConstraints>(
    engine: &TiKVBackend,
    model: &TitoModel<TiKVBackend, T>,
    value: T,
) -> Result<T, TitoError> {
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

pub async fn remove<T: TitoModelConstraints>(
    engine: &TiKVBackend,
    model: &TitoModel<TiKVBackend, T>,
    id: &str,
) -> Result<bool, TitoError> {
    engine
        .transaction(|tx| {
            let model = model.clone();
            let id = id.to_string();
            async move { model.remove(&id, &tx).await }
        })
        .await
}
