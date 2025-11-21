# Tito

> ⚠️ **WARNING: This project is in early development and NOT PRODUCTION READY.** Use at your own risk. The API may change without notice, and there may be bugs and performance issues. Not recommended for critical applications.

Tito is a powerful, flexible database layer built on top of TiKV, providing robust indexing strategies, relationship modeling, and transaction management capabilities for Rust applications.

## Features

- **Powerful Indexing Strategies**: Define custom indexes with conditional logic for efficient querying
- **Relationship Modeling**: Create and manage embedded relationships between data models
- **Transaction Management**: Full ACID-compliant transaction support
- **Event-Driven Queue**: Built-in persistent event queue with FIFO ordering and partition-based parallelism
- **Async Workers**: Tokio-powered workers for concurrent event processing with per-event-type isolation
- **Type Safety**: Leverages Rust's type system for safety and performance
- **Flexible Query API**: Query data using intuitive builder pattern

## Installation

Add Tito to your project:

```toml
[dependencies]
tito = "0.1.43"
```

## Quick Start

### Setting up a Connection

```rust
// Connect to TiKV with multiple endpoints
let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;

// For production, use multiple PD endpoints for high availability
let tito_db = TiKV::connect(vec![
    "pd1.example.com:2379",
    "pd2.example.com:2379",
    "pd3.example.com:2379"
]).await?;
```

### Creating a Model

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn relationships(&self) -> Vec<tito::types::TitoRelationshipConfig> {
        vec![] // No relationships for this simple model
    }

    fn references(&self) -> Vec<String> {
        vec![] // No references to other entities
    }

    fn partition_key(&self) -> String {
        self.id.clone() // Partition by user ID
    }

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
        vec![] // No events for this model
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

// Create model with the storage backend  
let user_model = tito_db.clone().model::<User>();
```

### Basic CRUD Operations

```rust
// Create a user
let user_id = DBUuid::new_v4().to_string();
let user = User {
    id: user_id.clone(),
    name: "John Doe".to_string(),
    email: "john@example.com".to_string(),
};

// Create user with transaction
let saved_user = tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        user_model.build(user, &tx).await
    }
}).await?;

// Find user by ID
let found_user = user_model.find_by_id(&user_id, vec![]).await?;

// Update user
let updated_user = User {
    id: user_id.clone(),
    name: "John Updated".to_string(),
    email: "john_updated@example.com".to_string(),
};

tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        user_model.update(updated_user, &tx).await
    }
}).await?;

// Delete user
tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        user_model.delete_by_id(&user_id, &tx).await
    }
}).await?;
```

## Using the Query Builder

Tito provides a powerful query builder pattern for easier and more readable querying.

### Basic Query

```rust
// Find user by email
let mut query = user_model.query_by_index("by_email");
let result = query
    .value("john@example.com")
    .execute()
    .await?;

// Check if we found a user
if let Some(user) = result.items.first() {
    println!("Found user: {}", user.name);
}
```

### Query with Relationships

```rust
// Post model with tag relationships
let mut query = post_model.query_by_index("post-by-author");
let posts = query
    .value("john")              // Author name
    .relationship("tags")       // Include tags relationship
    .limit(Some(10))            // Limit to 10 results
    .execute()
    .await?;

for post in posts.items {
    println!("Post: {} with {} tags", post.title, post.tags.len());
}
```

### Advanced Queries

```rust
// Find active users by role
let mut query = user_model.query_by_index("by_role_and_status");
let active_admins = query
    .value("admin")             // First index field (role)
    .value("active")            // Second index field (status)
    .limit(Some(20))            // Limit results
    .execute()
    .await?;

// Use cursor for pagination
if let Some(cursor) = active_admins.cursor {
    // Get next page using the same query with a cursor
    let mut next_page_query = user_model.query_by_index("by_role_and_status");
    let next_page = next_page_query
        .value("admin")
        .value("active")
        .cursor(Some(cursor))   // Pass the cursor
        .limit(Some(20))
        .execute()
        .await?;
}
```

### Reverse Order Query

```rust
// Get latest posts in reverse chronological order
let mut query = post_model.query_by_index("post-by-created");
let latest_posts = query
    .value("2023")              // Index by year
    .execute_reverse()          // Execute in reverse order
    .await?;
```

### Transaction-Specific Queries

```rust
tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        // Query within transaction context
        let mut query = user_model.query_by_index("by_email");
        let user = query
            .value("john@example.com")
            .execute_tx(&tx)        // Execute within transaction
            .await?;

        // Update user in same transaction
        if let Some(mut user) = user.items.first().cloned() {
            user.name = "John Smith".to_string();
            user_model.update(user, &tx).await?;
        }

        Ok::<_, TitoError>(())
    }
}).await?;
```

## Advanced Features

### Conditional Indexing

Tito lets you conditionally index data based on business rules:

```rust
TitoIndexConfig {
    condition: self.status == "active", // Only index active items
    name: "by_active_status".to_string(),
    fields: vec![TitoIndexField {
        name: "status".to_string(),
        r#type: TitoIndexBlockType::String,
    }],
}
```

### Relationship Modeling

Define relationships between models and fetch related data efficiently:

```rust
// Post model with references to multiple tags
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct Post {
    id: String,
    title: String,
    content: String,
    author: String,
    tag_ids: Vec<String>, // Reference to related tag IDs
    #[serde(default)]
    tags: Vec<Tag>,       // Will be populated when relationship is fetched
}

impl TitoModelTrait for Post {
    fn relationships(&self) -> Vec<TitoRelationshipConfig> {
        // Define the relationship between posts and tags
        vec![TitoRelationshipConfig {
            source_field_name: "tag_ids".to_string(),
            destination_field_name: "tags".to_string(),
            model: "tag".to_string(),
        }]
    }

    fn references(&self) -> Vec<String> {
        self.tag_ids.clone() // References to tag IDs
    }

    fn partition_key(&self) -> String {
        self.id.clone()
    }

    // ... other implementation details (indexes, table, events, id)
}

// Fetch a post with related tags using query builder
let mut query = post_model.query_by_index("post-by-id");
let post = query
    .value(post_id)
    .relationship("tags")
    .execute()
    .await?;
```

### Composite Indexes

Create and query using composite indexes:

```rust
// Define composite index
TitoIndexConfig {
    condition: true,
    name: "by_category_and_date".to_string(),
    fields: vec![
        TitoIndexField {
            name: "category".to_string(),
            r#type: TitoIndexBlockType::String,
        },
        TitoIndexField {
            name: "published_at".to_string(),
            r#type: TitoIndexBlockType::Number,
        },
    ],
}

// Query using composite index
let mut query = article_model.query_by_index("by_category_and_date");
let articles = query
    .value("technology")         // category value
    .value("20230101")           // published_at value as number
    .limit(Some(5))
    .execute()
    .await?;
```

## Event Queue System

Tito includes a built-in event-driven queue system for asynchronous processing with FIFO ordering and partition-based parallelism.

### Defining Events

Define events in your model by implementing the `events()` method:

```rust
impl TitoModelTrait for User {
    fn events(&self) -> Vec<TitoEventConfig> {
        vec![TitoEventConfig {
            name: "user".to_string(), // Event type name
        }]
    }

    // Other trait methods...
}
```

### Creating Events

Events are automatically created when you perform operations with event options:

```rust
use tito::{TitoOptions, TitoOperation};

// Create a user and generate an INSERT event
tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        user_model
            .build_with_options(
                user,
                TitoOptions::with_events(TitoOperation::Insert),
                &tx,
            )
            .await?;
        Ok::<_, TitoError>(())
    }
}).await?;

// Update a user and generate an UPDATE event
tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        user_model
            .update_with_options(
                user,
                TitoOptions::with_events(TitoOperation::Update),
                &tx,
            )
            .await?;
        Ok::<_, TitoError>(())
    }
}).await?;
```

### Setting Up Workers

Create workers to process events by event type:

```rust
use tito::queue::{TitoQueue, run_worker};
use tito::types::PartitionConfig;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::broadcast;

// Create queue
let queue = Arc::new(TitoQueue {
    engine: tito_db.clone(),
});

// Worker configuration
let is_leader = Arc::new(AtomicBool::new(true));
let partition_config = PartitionConfig {
    start: 0,
    end: 1024, // Process all 1024 partitions
};

// Create shutdown channel
let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

// Define event handler
let handler = move |event: tito::TitoEvent| {
    Box::pin(async move {
        println!("Processing: {} - {}", event.action, event.entity);
        // Your event processing logic here
        Ok::<_, TitoError>(())
    }) as BoxFuture<'static, Result<(), TitoError>>
};

// Start worker for "user" events
let worker_handle = run_worker(
    queue.clone(),
    String::from("user"), // Event type to process
    handler,
    partition_config,
    is_leader.clone(),
    5, // Concurrency level
    shutdown_rx,
)
.await;

// Gracefully shutdown when done
let _ = shutdown_tx.send(());
let _ = worker_handle.await;
```

### Key Concepts

- **Event Types**: Each model can define multiple event types. Workers process one event type at a time, enabling independent scaling and failure isolation.
- **FIFO Ordering**: Events are processed oldest-first within each partition, ensuring correct ordering.
- **Partitions**: The system uses 1024 fixed partitions for parallel processing. Partitioning is determined by the model's `partition_key()` method.
- **Checkpoints**: Each event type maintains independent checkpoints per partition, tracking processing progress.
- **Leader Election**: Use the `is_leader` flag to ensure only one worker processes events in distributed setups.
- **Graceful Shutdown**: Workers respect shutdown signals for clean termination.

### Multiple Workers

You can run multiple workers for different event types:

```rust
// Worker for user events
let user_worker = run_worker(
    queue.clone(),
    String::from("user"),
    user_handler,
    partition_config.clone(),
    is_leader.clone(),
    5,
    shutdown_rx.resubscribe(),
).await;

// Worker for order events
let order_worker = run_worker(
    queue.clone(),
    String::from("order"),
    order_handler,
    partition_config.clone(),
    is_leader.clone(),
    10, // Different concurrency
    shutdown_rx.resubscribe(),
).await;
```

Each event type acts as an independent queue (similar to Kafka topics), allowing you to:
- Scale workers independently based on load
- Isolate failures to specific event types
- Configure different concurrency levels per event type

## Running the Examples

Tito comes with comprehensive examples demonstrating all major features. To run them, you'll need a TiKV server running locally.

### Setting up TiKV

The quickest way to get started is using Docker:

```bash
# Start a local TiKV cluster
docker run -d --name tikv \
  -p 2379:2379 \
  pingcap/pd:latest \
  --name "pd" \
  --data-dir "/data/pd" \
  --client-urls "http://0.0.0.0:2379"

docker run -d --name tikv \
  -p 20160:20160 \
  --link pd \
  pingcap/tikv:latest \
  --pd "pd:2379" \
  --data-dir "/data/tikv"
```

Or use TiUP for a production-like setup:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
tiup playground --mode tikv-slim
```

### Example 1: Basic CRUD (`crud.rs`)

Demonstrates fundamental database operations:

```bash
cargo run --example crud
```

**What it does:**
- Connects to TiKV
- Creates a user with email indexing
- Finds the user by ID
- Updates user information
- Deletes the user

**Expected output:**
```
Created user: User { id: "...", name: "John Doe", email: "john@example.com" }
Found user: User { id: "...", name: "John Doe", email: "john@example.com" }
User updated successfully
User deleted successfully
```

### Example 2: Relationships and Queries (`blog.rs`)

Shows how to work with related data using the powerful relationship system:

```bash
cargo run --example blog
```

**What it does:**
- Creates tags (Technology, Travel, Rust, Databases)
- Creates blog posts with multiple tags
- Demonstrates relationship hydration
- Queries posts by tag
- Queries posts by author

**Expected output:**
```
Created tags:
- Technology: <uuid>
- Travel: <uuid>
...

Created posts:
1. Introduction to TiKV (by Alice)
2. Best cities to visit in Europe (by Bob)
3. Using Rust with TiKV (by Alice)

Post with tags:
Title: Introduction to TiKV
Tags:
- Technology
- Databases

Technology posts:
- Using Rust with TiKV (by Alice)
  Tags: Technology, Rust, Databases
...
```

**Key concepts demonstrated:**
- One-to-many relationships (Post → Tags)
- Relationship hydration with `.relationship("tags")`
- Composite indexes for efficient querying
- Query builder pattern

### Example 3: Event Queue System (`queue_fifo.rs`)

Demonstrates the event-driven queue with FIFO processing:

```bash
cargo run --example queue_fifo
```

**What it does:**
- Creates 5 users with events enabled
- Starts a worker to process events
- Shows FIFO ordering (oldest events first)
- Demonstrates partition-based parallelism
- Graceful shutdown

**Expected output:**
```
🚀 Testing FIFO Queue with Checkpoints

📝 Creating 5 users to generate events...
✓ Created: User 1 (user1@example.com)
✓ Created: User 2 (user2@example.com)
...

📊 Setting up queue worker...
⚙️  Worker started! Processing events in FIFO order...

  [1] Processing event: INSERT - table:user:xxx (sequence: 00001...)
  [2] Processing event: INSERT - table:user:xxx (sequence: 00001...)
  ...

✅ Complete! Processed 5 events in FIFO order
   (Oldest events processed first!)
```

**Key concepts demonstrated:**
- Event generation with `TitoOptions::with_events()`
- Event worker setup with `run_worker()`
- Event type isolation ("user" events)
- FIFO ordering within partitions
- Checkpoint-based progress tracking
- Graceful shutdown handling

### How the Queue System Works

The event queue is perfect for:
- **Async processing**: Offload heavy work from request handlers
- **Event sourcing**: Track all changes to your data
- **Multi-tenant processing**: Partition by tenant ID for isolation
- **Saga patterns**: Coordinate distributed transactions

**Architecture:**

```
Model Change (INSERT/UPDATE/DELETE)
         ↓
Event Generated (if events() enabled)
         ↓
Stored in TiKV: event:{event_type}:{partition}:{sequence}
         ↓
Worker polls: queue:{event_type}:PENDING:{partition}:{sequence}
         ↓
Process Event (your handler logic)
         ↓
Mark Complete: queue:{event_type}:COMPLETED:{partition}:{sequence}
         ↓
Update Checkpoint: queue_checkpoint:{event_type}:{partition}
```

**Practical Example - Email Notifications:**

```rust
// 1. Define event in your User model
impl TitoModelTrait for User {
    fn events(&self) -> Vec<TitoEventConfig> {
        vec![TitoEventConfig {
            name: "user".to_string(),
        }]
    }

    fn partition_key(&self) -> String {
        // Partition by user_id for ordered processing per user
        self.id.clone()
    }
}

// 2. Create user with event
tito_db.transaction(|tx| {
    let user_model = user_model.clone();
    async move {
        user_model.build_with_options(
            new_user,
            TitoOptions::with_events(TitoOperation::Insert),
            &tx,
        ).await?;
        Ok::<_, TitoError>(())
    }
}).await?;

// 3. Worker processes event asynchronously
let handler = move |event: TitoEvent| {
    Box::pin(async move {
        if event.action == "INSERT" {
            // Send welcome email
            send_welcome_email(&event.entity_id()).await?;
        }
        Ok::<_, TitoError>(())
    }) as BoxFuture<'static, Result<(), TitoError>>
};

let worker = run_worker(
    queue.clone(),
    String::from("user"), // Only process "user" events
    handler,
    partition_config,
    is_leader,
    5, // Process 5 events concurrently
    shutdown_rx,
).await;
```

**Scaling Pattern:**

```rust
// Server 1: Partitions 0-511
let partition_config = PartitionConfig { start: 0, end: 512 };

// Server 2: Partitions 512-1023
let partition_config = PartitionConfig { start: 512, end: 1024 };

// Both process same event type, different partitions
// = Horizontal scalability + load balancing
```

## Requirements

- Rust 2021 edition or later
- TiKV server running (local or remote)

## License

This project is licensed under the Apache License, Version 2.0.
