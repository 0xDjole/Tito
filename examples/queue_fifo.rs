use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tito::{
    queue::{run_worker, EventType, QueueConfig, QueueEvent, TitoQueue},
    types::DBUuid,
    TiKV, TitoError, WorkerConfig,
};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    name: String,
    email: String,
    action: String,
}

impl EventType for UserEvent {
    fn event_type_name() -> &'static str {
        "user"
    }

    fn variant_name(&self) -> &'static str {
        "user_created"
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    println!("Testing FIFO Queue\n");

    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;

    let queue = Arc::new(TitoQueue::new(
        tito_db.clone(),
        QueueConfig::with_partitions(1),
    ));

    println!("Publishing 5 events...\n");

    for i in 1..=5 {
        let user_id = DBUuid::new_v4().to_string();
        let event = QueueEvent::new(
            format!("user:{}", user_id),
            UserEvent {
                user_id,
                name: format!("User {}", i),
                email: format!("user{}@example.com", i),
                action: "created".to_string(),
            },
        );

        queue.publish(event).await?;
        println!("Published event for User {}", i);
    }

    println!("\nStarting worker...\n");

    let events_processed = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let events_processed_clone = events_processed.clone();

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let handler = move |event: QueueEvent<UserEvent>| {
        let counter = events_processed_clone.clone();
        Box::pin(async move {
            let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "[{}] {} - {} ({})",
                count,
                event.payload.action,
                event.payload.name,
                event.payload.email,
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
            partition_range: 0..1,
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
