use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tito::{
    queue::{run_worker, TitoQueue},
    types::{
        DBUuid, TitoEngine, TitoEventConfig, TitoIndexBlockType, TitoIndexConfig, TitoIndexField,
        TitoModelTrait, PartitionConfig,
    },
    EventConfig, TiKV, TitoError, TitoModel, TitoOperation, TitoOptions,
};

// Simple User model with events enabled
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn relationships(&self) -> Vec<tito::types::TitoRelationshipConfig> {
        vec![]
    }

    fn references(&self) -> Vec<String> {
        vec![]
    }

    fn partition_key(&self) -> String {
        // Partition by user ID for per-user ordering
        self.id.clone()
    }

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
        vec![TitoEventConfig {
            name: "user.changed".to_string(),
        }]
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    println!("🚀 Testing FIFO Queue with Checkpoints\n");

    // Connect to TiKV
    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;
    let user_model = tito_db.clone().model::<User>();

    println!("📝 Creating 5 users to generate events...\n");

    // Create users with events enabled
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

        println!("✓ Created: {} ({})", user.name, user.email);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    println!("\n📊 Setting up queue worker...\n");

    // Create queue
    let queue = Arc::new(TitoQueue {
        engine: tito_db.clone(),
    });

    // Worker configuration
    let is_leader = Arc::new(AtomicBool::new(true));
    let partition_config = PartitionConfig {
        start: 0,
        end: 1024, // Process all partitions
    };

    // Event counter
    let events_processed = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let events_processed_clone = events_processed.clone();

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    // Event handler
    let handler = move |event: tito::TitoEvent| {
        let counter = events_processed_clone.clone();
        Box::pin(async move {
            let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "  [{}] Processing event: {} - {} (sequence: {})",
                count,
                event.action,
                event.entity,
                event.key.split(':').last().unwrap_or("unknown")
            );

            // Simulate some processing
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            Ok::<_, TitoError>(())
        }) as BoxFuture<'static, Result<(), TitoError>>
    };

    // Start worker
    let worker_handle = run_worker(
        queue.clone(),
        String::from("user.changed"), // event_type - matches the event config
        handler,
        partition_config,
        is_leader.clone(),
        5, // concurrency
        shutdown_rx,
    )
    .await;

    println!("⚙️  Worker started! Processing events in FIFO order...\n");

    // Wait for events to be processed
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Shutdown worker
    println!("\n🛑 Shutting down worker...");
    let _ = shutdown_tx.send(());
    let _ = worker_handle.await;

    let total = events_processed.load(Ordering::SeqCst);
    println!("\n✅ Complete! Processed {} events in FIFO order", total);
    println!("   (Oldest events processed first!)");

    Ok(())
}
