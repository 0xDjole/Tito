use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tito::{
    connect,
    transaction::TransactionManager,
    types::{
        DBUuid, TitoConfigs, TitoEmbeddedRelationshipConfig, TitoError, TitoFindByIndexPayload,
        TitoIndexBlockType, TitoIndexConfig, TitoIndexField, TitoModelTrait, TitoUtilsConnectInput,
        TitoUtilsConnectPayload,
    },
    BaseTito, TitoModel,
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

    fn get_event_table_name(&self) -> Option<String> {
        None
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
            // Index posts by author
            TitoIndexConfig {
                condition: true,
                name: "post-by-author".to_string(),
                fields: vec![TitoIndexField {
                    name: "author".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
                custom_generator: None,
            },
            // Index posts by title
            TitoIndexConfig {
                condition: true,
                name: "post-by-title".to_string(),
                fields: vec![TitoIndexField {
                    name: "title".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
                custom_generator: None,
            },
            // Custom index generator for posts by tag
            // This creates an entry for each tag the post has
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

    fn get_event_table_name(&self) -> Option<String> {
        None
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

// Blog service to handle posts and tags
struct BlogService {
    transaction_manager: TransactionManager,
    post_model: TitoModel<Post>,
    tag_model: TitoModel<Tag>,
}

impl BlogService {
    fn new(
        transaction_manager: TransactionManager,
        post_model: TitoModel<Post>,
        tag_model: TitoModel<Tag>,
    ) -> Self {
        Self {
            transaction_manager,
            post_model,
            tag_model,
        }
    }

    // Create a new tag
    async fn create_tag(&self, name: String, description: String) -> Result<Tag, TitoError> {
        let tag_id = DBUuid::new_v4().to_string();

        self.transaction_manager
            .transaction(|tx| {
                let tag_model = &self.tag_model;

                async move {
                    let tag = Tag {
                        id: tag_id,
                        name,
                        description,
                    };

                    tag_model.build(tag.clone(), &tx).await?;
                    Ok::<_, TitoError>(tag)
                }
            })
            .await
    }

    // Create a new post with multiple tags
    async fn create_post(
        &self,
        title: String,
        content: String,
        author: String,
        tag_ids: Vec<String>,
    ) -> Result<Post, TitoError> {
        let post_id = DBUuid::new_v4().to_string();

        self.transaction_manager
            .transaction(|tx| {
                let post_model = &self.post_model;

                async move {
                    let post = Post {
                        id: post_id,
                        title,
                        content,
                        author,
                        tag_ids,
                        tags: Vec::new(),
                    };

                    post_model.build(post.clone(), &tx).await?;
                    Ok::<_, TitoError>(post)
                }
            })
            .await
    }

    // Get a post with all its tags
    async fn get_post_with_tags(&self, post_id: &str) -> Result<Post, TitoError> {
        // Find the post with the tags relationship included
        self.post_model
            .find_by_id(post_id, vec!["tags".to_string()])
            .await
    }

    // Find posts by tag
    async fn find_posts_by_tag(&self, tag_id: &str) -> Result<Vec<Post>, TitoError> {
        // Use the post-by-tag index to find posts with this tag
        let posts = self
            .post_model
            .find_by_index(TitoFindByIndexPayload {
                index: "post-by-tag".to_string(),
                values: vec![tag_id.to_string()],
                rels: vec!["tags".to_string()], // Include all tags in the results
                limit: None,
                cursor: None,
                end: None,
            })
            .await?;

        Ok(posts.items)
    }

    // Find posts by author
    async fn find_posts_by_author(&self, author: &str) -> Result<Vec<Post>, TitoError> {
        // Use the post-by-author index to find posts by this author
        let posts = self
            .post_model
            .find_by_index(TitoFindByIndexPayload {
                index: "post-by-author".to_string(),
                values: vec![author.to_string()],
                rels: vec!["tags".to_string()], // Include all tags in the results
                limit: None,
                cursor: None,
                end: None,
            })
            .await?;

        Ok(posts.items)
    }

    // Add a tag to a post
    async fn add_tag_to_post(&self, post_id: &str, tag_id: &str) -> Result<Post, TitoError> {
        self.transaction_manager
            .transaction(|tx| {
                let post_model = &self.post_model;

                async move {
                    // Get the current post
                    let mut post = post_model.find_by_id_tx(post_id, vec![], &tx).await?;

                    // Add the tag ID if it's not already there
                    if !post.tag_ids.contains(&tag_id.to_string()) {
                        post.tag_ids.push(tag_id.to_string());

                        // Update the post
                        post_model.update(post.clone(), &tx).await?;
                    }

                    Ok::<_, TitoError>(post)
                }
            })
            .await
    }

    // Remove a tag from a post
    async fn remove_tag_from_post(&self, post_id: &str, tag_id: &str) -> Result<Post, TitoError> {
        self.transaction_manager
            .transaction(|tx| {
                let post_model = &self.post_model;

                async move {
                    // Get the current post
                    let mut post = post_model.find_by_id_tx(post_id, vec![], &tx).await?;

                    // Remove the tag ID if it exists
                    post.tag_ids.retain(|id| id != tag_id);

                    // Update the post
                    post_model.update(post.clone(), &tx).await?;

                    Ok::<_, TitoError>(post)
                }
            })
            .await
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
    let tx_manager = TransactionManager::new(Arc::new(tikv_client.clone()));

    // Create models
    let post_model = TitoModel::<Post>::new(configs.clone(), tx_manager.clone());
    let tag_model = TitoModel::<Tag>::new(configs.clone(), tx_manager.clone());

    // Create blog service
    let blog_service = BlogService::new(tx_manager.clone(), post_model.clone(), tag_model.clone());

    // Create some tags
    let tech_tag = blog_service
        .create_tag("Technology".to_string(), "All about tech".to_string())
        .await?;

    let travel_tag = blog_service
        .create_tag(
            "Travel".to_string(),
            "Adventures around the world".to_string(),
        )
        .await?;

    let rust_tag = blog_service
        .create_tag("Rust".to_string(), "Rust programming language".to_string())
        .await?;

    let database_tag = blog_service
        .create_tag(
            "Databases".to_string(),
            "Database systems and technologies".to_string(),
        )
        .await?;

    println!("Created tags:");
    println!("- {}: {}", tech_tag.name, tech_tag.id);
    println!("- {}: {}", travel_tag.name, travel_tag.id);
    println!("- {}: {}", rust_tag.name, rust_tag.id);
    println!("- {}: {}", database_tag.name, database_tag.id);

    // Create some posts with multiple tags
    let post1 = blog_service
        .create_post(
            "Introduction to TiKV".to_string(),
            "TiKV is a distributed key-value storage system...".to_string(),
            "Alice".to_string(),
            vec![tech_tag.id.clone(), database_tag.id.clone()], // Multiple tags
        )
        .await?;

    let post2 = blog_service
        .create_post(
            "Best cities to visit in Europe".to_string(),
            "Europe offers a diverse range of cities...".to_string(),
            "Bob".to_string(),
            vec![travel_tag.id.clone()], // Single tag
        )
        .await?;

    let post3 = blog_service
        .create_post(
            "Using Rust with TiKV".to_string(),
            "Here are some examples of using Rust with TiKV...".to_string(),
            "Alice".to_string(),
            vec![
                tech_tag.id.clone(),
                rust_tag.id.clone(),
                database_tag.id.clone(),
            ], // Multiple tags
        )
        .await?;

    println!("\nCreated posts:");
    println!("1. {} (by {})", post1.title, post1.author);
    println!("2. {} (by {})", post2.title, post2.author);
    println!("3. {} (by {})", post3.title, post3.author);

    // Add an additional tag to post2
    let updated_post2 = blog_service
        .add_tag_to_post(&post2.id, &tech_tag.id)
        .await?;

    println!("\nAdded 'Technology' tag to post2");

    // Get a post with all its tags
    let post_with_tags = blog_service.get_post_with_tags(&post1.id).await?;
    println!("\nPost with tags:");
    println!("Title: {}", post_with_tags.title);
    println!("Tags:");
    for tag in &post_with_tags.tags {
        println!("- {}", tag.name);
    }

    // Find posts by tag
    let tech_posts = blog_service.find_posts_by_tag(&tech_tag.id).await?;
    println!("\nTechnology posts:");
    for post in &tech_posts {
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

    let travel_posts = blog_service.find_posts_by_tag(&travel_tag.id).await?;
    println!("\nTravel posts:");
    for post in &travel_posts {
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

    // Find posts by author
    let alice_posts = blog_service.find_posts_by_author("Alice").await?;
    println!("\nAlice's posts:");
    for post in &alice_posts {
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

    // Remove a tag from a post
    let post3_without_tech = blog_service
        .remove_tag_from_post(&post3.id, &tech_tag.id)
        .await?;

    println!("\nRemoved 'Technology' tag from post3");

    // Verify the tag has been removed
    let updated_post3 = blog_service.get_post_with_tags(&post3.id).await?;
    println!(
        "Updated post3 tags: {}",
        updated_post3
            .tags
            .iter()
            .map(|t| t.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Show which posts have the 'Rust' tag after all our changes
    let rust_posts = blog_service.find_posts_by_tag(&rust_tag.id).await?;
    println!("\nRust posts after all changes:");
    for post in &rust_posts {
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

    Ok(())
}
