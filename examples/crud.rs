use serde::{Deserialize, Serialize};
use tito::{
    types::{DBUuid, TitoEngine, TitoModelTrait},
    Guard, TiKV, TitoError, TitoIndex, TitoIndexField, TitoIndexValue,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: "user-by-email",
            fields: vec![TitoIndexField::Text(self.email.clone())],
            value: TitoIndexValue::Full,
            unique: true,
        }]
    }

    fn table() -> String {
        "users".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;
    let users = tito_db.clone().model::<User>();

    let user_id = DBUuid::new_v4().to_string();
    let user = User {
        id: user_id.clone(),
        name: "John Doe".to_string(),
        email: "john@example.com".to_string(),
    };

    tito_db
        .transaction(|tx| {
            let users = users.clone();
            let user = user.clone();
            async move { users.create(&user, &tx).await }
        })
        .await?;
    println!("Created user: {:?}", user);

    let found = tito_db
        .transaction(|tx| {
            let users = users.clone();
            let user_id = user_id.clone();
            async move { tx.batch_get(users.read(&user_id, Guard::None)).await }
        })
        .await?;
    println!("Found user: {:?}", found);

    let by_email = tito_db
        .transaction(|tx| {
            let users = users.clone();
            async move {
                tx.batch_get(users.read_unique(
                    "user-by-email",
                    vec![TitoIndexField::Text("john@example.com".to_string())],
                    Guard::None,
                ))
                .await
            }
        })
        .await?;
    println!("Found user by email: {:?}", by_email);

    tito_db
        .transaction(|tx| {
            let users = users.clone();
            let user_id = user_id.clone();
            async move {
                let old = tx.batch_get(users.read(&user_id, Guard::Update)).await?;
                let new = User {
                    name: "John Updated".to_string(),
                    email: "john_updated@example.com".to_string(),
                    ..old.clone()
                };
                users.update(&old, &new, &tx).await
            }
        })
        .await?;
    println!("User updated");

    tito_db
        .transaction(|tx| {
            let users = users.clone();
            let user_id = user_id.clone();
            async move {
                let user = tx.batch_get(users.read(&user_id, Guard::None)).await?;
                users.remove(&user, &tx).await
            }
        })
        .await?;
    println!("User deleted");

    Ok(())
}
