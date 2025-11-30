use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tito::{
    queue::{run_worker, TitoQueue},
    types::{
        DBUuid, TitoEngine, TitoEventConfig, TitoIndexBlockType, TitoIndexConfig, TitoIndexField,
        TitoModelTrait, WorkerConfig,
    },
    TiKV, TitoError, TitoOperation, TitoOptions,
};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "user-by-email".to_string(),
            fields: vec![TitoIndexField {
                name: "email".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
        }]
    }

    fn table(&self) -> String {
        "user".to_string()
    }

    fn events(&self) -> Vec<TitoEventConfig> {
        let now = chrono::Utc::now().timestamp();
        vec![TitoEventConfig {
            name: "user".to_string(),
            timestamp: now,
        }]
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    println!("Testing FIFO Queue\n");

    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;
    let user_model = tito_db.clone().model::<User>();

    println!("Creating 5 users...\n");

    for i in 1..=5 {
        let user = User {
            id: DBUuid::new_v4().to_string(),
            name: format!("User {}", i),
            email: format!("user{}@example.com", i),
        };

        tito_db
            .transaction(|tx| {
                let user_model = user_model.clone();
                let user_clone = user.clone();
                async move {
                    user_model
                        .build_with_options(
                            user_clone,
                            TitoOptions::with_events(TitoOperation::Insert),
                            &tx,
                        )
                        .await?;
                    Ok::<_, TitoError>(())
                }
            })
            .await?;

        println!("Created: {} ({})", user.name, user.email);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    println!("\nStarting worker...\n");

    let queue = Arc::new(TitoQueue {
        engine: tito_db.clone(),
    });

    let events_processed = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let events_processed_clone = events_processed.clone();

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let handler = move |event: tito::TitoEvent| {
        let counter = events_processed_clone.clone();
        Box::pin(async move {
            let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "[{}] {} - {}",
                count,
                event.action,
                event.entity,
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Ok::<_, TitoError>(())
        }) as BoxFuture<'static, Result<(), TitoError>>
    };

    let worker_handle = run_worker(
        queue.clone(),
        WorkerConfig {
            event_type: String::from("user"),
            consumer: String::from("example-consumer"),
            partition: 0,
        },
        handler,
        shutdown_rx,
    )
    .await;

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("\nShutting down...");
    let _ = shutdown_tx.send(());
    let _ = worker_handle.await;

    let total = events_processed.load(Ordering::SeqCst);
    println!("\nProcessed {} events", total);

    Ok(())
}
