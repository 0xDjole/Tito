use tikv_client::TransactionClient;

use crate::{error::TitoError, types::TitoUtilsConnectInput};

pub async fn connect(input: TitoUtilsConnectInput) -> Result<TransactionClient, TitoError> {
    let pd_endpoints = vec![input.payload.uri];

    let client = TransactionClient::new(pd_endpoints)
        .await
        .map_err(|e| TitoError::ConnectionFailed(e.to_string()))?;

    Ok(client)
}
