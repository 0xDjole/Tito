# Tito

Tito is a powerful, flexible database layer built on top of TiKV, providing robust indexing strategies, relationship modeling, and transaction management capabilities for Rust applications.

## Features

- **Powerful Indexing Strategies**: Define custom indexes with conditional logic for efficient querying
- **Relationship Modeling**: Create and manage embedded relationships between data models
- **Transaction Management**: Full ACID-compliant transaction support
- **Type Safety**: Leverages Rust's type system for safety and performance
- **Flexible Query API**: Query data using multiple strategies based on your application needs

## Installation

Add Tito to your project:

```toml
[dependencies]
tito = "0.1.0"
```

## Quick Start

### Setting up a Connection

```rust
let tikv_client = connect(TitoUtilsConnectInput {
    payload: TitoUtilsConnectPayload {
        uri: "127.0.0.1:2379".to_string(),
    },
}).await?;

// Initialize config
let configs = TitoConfigs {
    is_read_only: Arc::new(AtomicBool::new(false)),
};

// Create transaction manager
let tx_manager = TransactionManager::new(Arc::new(tikv_client));
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

    fn get_event_table_name(&self) -> Option<String> {
        Some("jobs".to_string())
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

// Create model
let user_model = TitoModel::<User>::new(configs, tx_manager.clone());
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
let saved_user = tx_manager
    .transaction(|tx| {
        let model = &user_model;
        async move { model.build(user, &tx).await }
    })
    .await?;

// Find user
let found_user = user_model.find_by_id(&user_id, vec![]).await?;

// Update user
let updated_user = User {
    id: user_id.clone(),
    name: "John Updated".to_string(),
    email: "john_updated@example.com".to_string(),
};

tx_manager
    .transaction(|tx| {
        let model = &user_model;
        async move { model.update(updated_user, &tx).await }
    })
    .await?;

// Delete user
tx_manager
    .transaction(|tx| {
        let model = &user_model;
        async move { model.delete_by_id(&user_id, &tx).await }
    })
    .await?;
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

// Fetch a post with all its related tags
let post_with_tags = post_model
    .find_by_id(post_id, vec!["tags".to_string()]) // Include tags relationship
    .await?;
```

### Query by Index

Efficiently query data using custom indexes:

```rust
// Find posts by tag
let posts = post_model
    .find_by_index(TitoFindByIndexPayload {
        index: "post-by-tag".to_string(),
        values: vec![tag_id.to_string()],
        rels: vec!["tags".to_string()], // Include tags in results
        limit: None,
        cursor: None,
        end: None,
    })
    .await?;
```

## Examples

For more detailed examples, check out the examples directory:

- `crud.rs` - Basic CRUD operations
- `blog.rs` - More complex example with relationships and various indexing strategies

## Requirements

- Rust 2021 edition or later
- TiKV server running (local or remote)

## License

This project is licensed under the Apache License, Version 2.0.
