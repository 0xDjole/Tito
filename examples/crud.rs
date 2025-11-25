use serde::{Deserialize, Serialize};
use tito::{
    types::{
        DBUuid, TitoEngine, TitoEventConfig, TitoIndexBlockType, TitoIndexConfig, TitoIndexField,
        TitoModelTrait,
    },
    TiKV, TitoError,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "by_email".to_string(),
            fields: vec![TitoIndexField {
                name: "email".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
        }]
    }

    fn table(&self) -> String {
        "users".to_string()
    }

    fn events(&self) -> Vec<TitoEventConfig> {
        vec![]
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;
    let user_model = tito_db.clone().model::<User>();

    let user_id = DBUuid::new_v4().to_string();
    let user = User {
        id: user_id.clone(),
        name: "John Doe".to_string(),
        email: "john@example.com".to_string(),
    };

    let saved_user = tito_db
        .transaction(|tx| {
            let user_model = user_model.clone();
            async move { user_model.build(user, &tx).await }
        })
        .await?;

    println!("Created user: {:?}", saved_user);

    let found_user = user_model.find_by_id(&user_id, vec![]).await?;
    println!("Found user: {:?}", found_user);

    let updated_user = User {
        id: user_id.clone(),
        name: "John Updated".to_string(),
        email: "john_updated@example.com".to_string(),
    };

    tito_db
        .transaction(|tx| {
            let user_model = user_model.clone();
            async move { user_model.update(updated_user, &tx).await }
        })
        .await?;

    println!("User updated");

    tito_db
        .transaction(|tx| {
            let user_model = user_model.clone();
            async move { user_model.delete_by_id(&user_id, &tx).await }
        })
        .await?;

    println!("User deleted");

    Ok(())
}
