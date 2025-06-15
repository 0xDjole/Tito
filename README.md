# Tito

> ⚠️ **WARNING: This project is in early development and NOT PRODUCTION READY.** Use at your own risk. The API may change without notice, and there may be bugs and performance issues. Not recommended for critical applications.

Tito is a powerful, flexible database layer built on top of TiKV, providing robust indexing strategies, relationship modeling, and transaction management capabilities for Rust applications.

## Features

- **Powerful Indexing Strategies**: Define custom indexes with conditional logic for efficient querying
- **Relationship Modeling**: Create and manage embedded relationships between data models
- **Transaction Management**: Full ACID-compliant transaction support
- **Job Queue**: Built-in persistent job queue with retries and scheduling capabilities
- **Async Workers**: Tokio-powered workers for concurrent job processing
- **Type Safety**: Leverages Rust's type system for safety and performance
- **Flexible Query API**: Query data using intuitive builder pattern

## Installation

Add Tito to your project:

```toml
[dependencies]
tito = "0.1.8"
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
    fn get_embedded_relationships(&self) -> Vec<tito::types::TitoEmbeddedRelationshipConfig> {
        vec![] // No relationships for this simple model
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

    fn get_events(&self) -> Vec<TitoEventConfig> {
        vec![]
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

// Create model with the storage backend
let user_model: TitoModel<_, User> = TitoModel::new(tito_db.clone());
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
    custom_generator: None,
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
    fn get_embedded_relationships(&self) -> Vec<TitoEmbeddedRelationshipConfig> {
        // Define the relationship between posts and tags
        vec![TitoEmbeddedRelationshipConfig {
            source_field_name: "tag_ids".to_string(),
            destination_field_name: "tags".to_string(),
            model: "tag".to_string(),
        }]
    }

    // ... other implementation details
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
    custom_generator: None,
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

## Examples

For more detailed examples, check out the examples directory:

- `crud.rs` - Basic CRUD operations
- `blog.rs` - More complex example with relationships and query builder

## Requirements

- Rust 2021 edition or later
- TiKV server running (local or remote)

## License

This project is licensed under the Apache License, Version 2.0.
