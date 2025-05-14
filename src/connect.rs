use tikv_client::TransactionClient;

use crate::types::{TitoError, TitoUtilsConnectInput};

pub async fn connect(input: TitoUtilsConnectInput) -> Result<TransactionClient, TitoError> {
    let pd_endpoints = vec![input.payload.uri];

    let client = TransactionClient::new(pd_endpoints)
        .await
        .map_err(|e| TitoError::FailedToConnect(e.to_string()))?;

    Ok(client)
}
