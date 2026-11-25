use std::net::SocketAddr;

use crate::backend::tikv::{TiKV, TiKVBackend};
use crate::types::{TitoEngine, TitoTransaction};

pub(crate) async fn native_engine() -> TiKVBackend {
    let endpoint = std::env::var("TITO_NATIVE_PD_URI").expect(
        "TITO_NATIVE_PD_URI must name the disposable PD endpoint owned by the verification run",
    );
    let address: SocketAddr = endpoint
        .parse()
        .expect("The owned PD endpoint must be an explicit loopback host:port");
    assert!(
        address.ip().is_loopback() && address.port() != 0,
        "Queue tests require their declared disposable loopback endpoint"
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
        "Queue tests require an explicit bounded disposable run identity"
    );
    TiKV::connect(vec![endpoint])
        .await
        .expect("Connect to the test-owned native PD and TiKV")
}

pub(crate) fn unique_queue_name(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

pub(crate) async fn put_raw(engine: &TiKVBackend, key: &str, value: Vec<u8>) {
    engine
        .transaction(|tx| {
            let value = value.clone();
            async move { tx.put(key.as_bytes(), value).await }
        })
        .await
        .expect("Write a raw test row");
}

pub(crate) async fn get_raw(engine: &TiKVBackend, key: &str) -> Option<Vec<u8>> {
    engine
        .transaction(|tx| async move { tx.get(key.as_bytes()).await })
        .await
        .expect("Read a raw test row")
}

pub(crate) async fn keys_with_prefix(engine: &TiKVBackend, prefix: &str) -> Vec<String> {
    let mut end = prefix.as_bytes().to_vec();
    end.push(0xff);
    engine
        .transaction(|tx| {
            let end = end.clone();
            async move {
                tx.scan_keys(prefix.as_bytes().to_vec()..end, 10_000)
                    .await
                    .map(|keys| {
                        keys.into_iter()
                            .map(|key| String::from_utf8(key).expect("Queue keys are UTF-8"))
                            .collect::<Vec<_>>()
                    })
            }
        })
        .await
        .expect("Scan raw test rows")
}
