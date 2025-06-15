use serde::{Deserialize, Serialize};
use tito::{
    types::{
        DBUuid, TitoEmbeddedRelationshipConfig, TitoEngine, TitoEventConfig, TitoIndexBlockType,
        TitoIndexConfig, TitoIndexField, TitoModelTrait,
    },
    TiKV, TitoError, TitoModel,
};

// Simple Tag model
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct Tag {
    id: String,
    name: String,
    description: String,
}

// Post model with references to multiple tags
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct Post {
    id: String,
    title: String,
    content: String,
    author: String,

    // Vector of tag IDs - many-to-many relationship
    tag_ids: Vec<String>,

    // This will be populated when we use relationships
    #[serde(default)]
    tags: Vec<Tag>,
}

// Implement TitoModelTrait for Tag
impl TitoModelTrait for Tag {
    fn get_embedded_relationships(&self) -> Vec<TitoEmbeddedRelationshipConfig> {
        // Tags don't embed anything
        vec![]
    }

    fn get_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "tag-by-name".to_string(),
            fields: vec![TitoIndexField {
                name: "name".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
            custom_generator: None,
        }]
    }

    fn get_table_name(&self) -> String {
        "tag".to_string()
    }

    fn get_events(&self) -> Vec<TitoEventConfig> {
        vec![]
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

// Implement TitoModelTrait for Post
impl TitoModelTrait for Post {
    fn get_embedded_relationships(&self) -> Vec<TitoEmbeddedRelationshipConfig> {
        // Posts embed multiple tags
        vec![TitoEmbeddedRelationshipConfig {
            source_field_name: "tag_ids".to_string(),
            destination_field_name: "tags".to_string(),
            model: "tag".to_string(),
        }]
    }

    fn get_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "post-by-author".to_string(),
                fields: vec![TitoIndexField {
                    name: "author".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
                custom_generator: None,
            },
            TitoIndexConfig {
                condition: true,
                name: "post-by-title".to_string(),
                fields: vec![TitoIndexField {
                    name: "title".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
                custom_generator: None,
            },
            TitoIndexConfig {
                condition: true,
                name: "post-by-tag".to_string(),
                fields: vec![TitoIndexField {
                    name: "tag_ids".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
                custom_generator: None,
            },
        ]
    }

    fn get_table_name(&self) -> String {
        "post".to_string()
    }

    fn get_events(&self) -> Vec<TitoEventConfig> {
        vec![]
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    // Connect to TiKV
    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;

    // Create models
    let post_model = tito_db.clone().model::<Post>();
    let tag_model = tito_db.clone().model::<Tag>();

    // Create some tags
    let tech_tag = tito_db
        .transaction(|tx| {
            let tag_model = tag_model.clone();
            let tag = Tag {
                id: DBUuid::new_v4().to_string(),
                name: "Technology".to_string(),
                description: "All about tech".to_string(),
            };
            let tag_clone = tag.clone();

            async move {
                tag_model.build(tag_clone, &tx).await?;
                Ok::<_, TitoError>(tag)
            }
        })
        .await?;

    let travel_tag = tito_db
        .transaction(|tx| {
            let tag_model = tag_model.clone();
            let tag = Tag {
                id: DBUuid::new_v4().to_string(),
                name: "Travel".to_string(),
                description: "Adventures around the world".to_string(),
            };
            let tag_clone = tag.clone();

            async move {
                tag_model.build(tag_clone, &tx).await?;
                Ok::<_, TitoError>(tag)
            }
        })
        .await?;

    let rust_tag = tito_db
        .transaction(|tx| {
            let tag_model = tag_model.clone();
            let tag = Tag {
                id: DBUuid::new_v4().to_string(),
                name: "Rust".to_string(),
                description: "Rust programming language".to_string(),
            };
            let tag_clone = tag.clone();

            async move {
                tag_model.build(tag_clone, &tx).await?;
                Ok::<_, TitoError>(tag)
            }
        })
        .await?;

    let database_tag = tito_db
        .transaction(|tx| {
            let tag_model = tag_model.clone();
            let tag = Tag {
                id: DBUuid::new_v4().to_string(),
                name: "Databases".to_string(),
                description: "Database systems and technologies".to_string(),
            };
            let tag_clone = tag.clone();

            async move {
                tag_model.build(tag_clone, &tx).await?;
                Ok::<_, TitoError>(tag)
            }
        })
        .await?;

    println!("Created tags:");
    println!("- {}: {}", tech_tag.name, tech_tag.id);
    println!("- {}: {}", travel_tag.name, travel_tag.id);
    println!("- {}: {}", rust_tag.name, rust_tag.id);
    println!("- {}: {}", database_tag.name, database_tag.id);

    // Create some posts with multiple tags
    let post1 = tito_db
        .transaction(|tx| {
            let post_model = post_model.clone();
            let post = Post {
                id: DBUuid::new_v4().to_string(),
                title: "Introduction to TiKV".to_string(),
                content: "TiKV is a distributed key-value storage system...".to_string(),
                author: "Alice".to_string(),
                tag_ids: vec![tech_tag.id.clone(), database_tag.id.clone()],
                tags: Vec::new(),
            };
            let post_clone = post.clone();

            async move {
                post_model.build(post_clone, &tx).await?;
                Ok::<_, TitoError>(post)
            }
        })
        .await?;

    let post2 = tito_db
        .transaction(|tx| {
            let post_model = post_model.clone();
            let post = Post {
                id: DBUuid::new_v4().to_string(),
                title: "Best cities to visit in Europe".to_string(),
                content: "Europe offers a diverse range of cities...".to_string(),
                author: "Bob".to_string(),
                tag_ids: vec![travel_tag.id.clone()],
                tags: Vec::new(),
            };
            let post_clone = post.clone();

            async move {
                post_model.build(post_clone, &tx).await?;
                Ok::<_, TitoError>(post)
            }
        })
        .await?;

    let post3 = tito_db
        .transaction(|tx| {
            let post_model = post_model.clone();
            let post = Post {
                id: DBUuid::new_v4().to_string(),
                title: "Using Rust with TiKV".to_string(),
                content: "Here are some examples of using Rust with TiKV...".to_string(),
                author: "Alice".to_string(),
                tag_ids: vec![
                    tech_tag.id.clone(),
                    rust_tag.id.clone(),
                    database_tag.id.clone(),
                ],
                tags: Vec::new(),
            };
            let post_clone = post.clone();

            async move {
                post_model.build(post_clone, &tx).await?;
                Ok::<_, TitoError>(post)
            }
        })
        .await?;

    println!("\nCreated posts:");
    println!("1. {} (by {})", post1.title, post1.author);
    println!("2. {} (by {})", post2.title, post2.author);
    println!("3. {} (by {})", post3.title, post3.author);

    // Get a post with all its tags
    let post_with_tags = post_model
        .find_by_id(&post1.id, vec!["tags".to_string()])
        .await?;
    println!("\nPost with tags:");
    println!("Title: {}", post_with_tags.title);
    println!("Tags:");
    for tag in &post_with_tags.tags {
        println!("- {}", tag.name);
    }

    // Find posts by tag using the query builder
    let mut tech_query = post_model.query_by_index("post-by-tag");
    let tech_results = tech_query
        .value(tech_tag.id.clone())
        .relationship("tags")
        .execute()
        .await?;

    println!("\nTechnology posts:");
    for post in &tech_results.items {
        println!("- {} (by {})", post.title, post.author);
        println!(
            "  Tags: {}",
            post.tags
                .iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Find posts by author using the query builder
    let mut alice_query = post_model.query_by_index("post-by-author");
    let alice_results = alice_query
        .value("Alice".to_string())
        .relationship("tags")
        .execute()
        .await?;

    println!("\nAlice's posts:");
    for post in &alice_results.items {
        println!("- {}", post.title);
        println!(
            "  Tags: {}",
            post.tags
                .iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}
