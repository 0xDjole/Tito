// examples/basic_crud.rs
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tito::{
    connect,
    transaction::TransactionManager,
    types::{
        DBUuid, TitoConfigs, TitoError, TitoIndexBlockType, TitoIndexConfig, TitoIndexField,
        TitoModelTrait, TitoUtilsConnectInput, TitoUtilsConnectPayload,
    },
    BaseTito, TitoModel,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn get_embedded_relationships(&self) -> Vec<tito::types::TitoEmbeddedRelationshipConfig> {
        vec![]
    }

    fn get_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "by_email".to_string(),
            fields: vec![TitoIndexField {
                name: "email".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
            custom_generator: None,
        }]
    }

    fn get_table_name(&self) -> String {
        "users".to_string()
    }

    fn get_event_table_name(&self) -> Option<String> {
        None
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    // Connect to TiKV
    let tikv_client = connect(TitoUtilsConnectInput {
        payload: TitoUtilsConnectPayload {
            uri: "127.0.0.1:2379".to_string(),
        },
    })
    .await?;

    // Initialize config
    let configs = TitoConfigs {
        is_read_only: Arc::new(AtomicBool::new(false)),
    };

    // Create transaction manager
    let tx_manager = TransactionManager::new(Arc::new(tikv_client));

    // Create model
    let user_model = TitoModel::<User>::new(configs, tx_manager.clone());

    // Create a user
    let user_id = DBUuid::new_v4().to_string();
    let user = User {
        id: user_id.clone(),
        name: "John Doe".to_string(),
        email: "john@example.com".to_string(),
    };

    // Create user with transaction
    let saved_user = tx_manager
        .transaction(|tx| {
            let model = &user_model; // Use reference to avoid moving
            async move { model.build(user, &tx).await }
        })
        .await?;

    println!("Created user: {:?}", saved_user);

    // Find user (find_by_id already uses transaction internally)
    let found_user = user_model.find_by_id(&user_id, vec![]).await?;
    println!("Found user: {:?}", found_user);

    // Update user
    let updated_user = User {
        id: user_id.clone(),
        name: "John Updated".to_string(),
        email: "john_updated@example.com".to_string(),
    };

    tx_manager
        .transaction(|tx| {
            let model = &user_model; // Use reference to avoid moving
            async move { model.update(updated_user, &tx).await }
        })
        .await?;

    println!("User updated successfully");

    // Delete user
    tx_manager
        .transaction(|tx| {
            let model = &user_model; // Use reference to avoid moving
            async move { model.delete_by_id(&user_id, &tx).await }
        })
        .await?;

    println!("User deleted successfully");

    Ok(())
}
