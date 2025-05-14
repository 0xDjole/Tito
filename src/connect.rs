use tikv_client::TransactionClient;

use crate::types::{TiKvError, TiKvUtilsConnectInput};

pub async fn connect(input: TiKvUtilsConnectInput) -> Result<TransactionClient, TiKvError> {
    let pd_endpoints = vec![input.payload.uri];

    let client = TransactionClient::new(pd_endpoints)
        .await
        .map_err(|e| TiKvError::FailedToConnect(e.to_string()))?;

    Ok(client)
}
