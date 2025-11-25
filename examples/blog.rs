use serde::{Deserialize, Serialize};
use tito::{
    types::{
        DBUuid, TitoEngine, TitoEventConfig, TitoIndexBlockType,
        TitoIndexConfig, TitoIndexField, TitoModelTrait,
    },
    TiKV, TitoError,
};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct Tag {
    id: String,
    name: String,
    description: String,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct Post {
    id: String,
    title: String,
    content: String,
    author: String,
    tag_ids: Vec<String>,
    #[serde(default)]
    tags: Vec<Tag>,
}

impl TitoModelTrait for Tag {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "tag-by-name".to_string(),
            fields: vec![TitoIndexField {
                name: "name".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
        }]
    }

    fn table(&self) -> String {
        "tag".to_string()
    }

    fn events(&self) -> Vec<TitoEventConfig> {
        vec![]
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

impl TitoModelTrait for Post {
    fn relationships(&self) -> Vec<tito::types::TitoRelationshipConfig> {
        vec![tito::types::TitoRelationshipConfig {
            source_field_name: "tag_ids".to_string(),
            destination_field_name: "tags".to_string(),
            model: "tag".to_string(),
        }]
    }

    fn references(&self) -> Vec<String> {
        self.tag_ids.clone()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "post-by-author".to_string(),
                fields: vec![TitoIndexField {
                    name: "author".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "post-by-tag".to_string(),
                fields: vec![TitoIndexField {
                    name: "tag_ids".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
        ]
    }

    fn table(&self) -> String {
        "post".to_string()
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

    let post_model = tito_db.clone().model::<Post>();
    let tag_model = tito_db.clone().model::<Tag>();

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

    let rust_tag = tito_db
        .transaction(|tx| {
            let tag_model = tag_model.clone();
            let tag = Tag {
                id: DBUuid::new_v4().to_string(),
                name: "Rust".to_string(),
                description: "Rust programming".to_string(),
            };
            let tag_clone = tag.clone();
            async move {
                tag_model.build(tag_clone, &tx).await?;
                Ok::<_, TitoError>(tag)
            }
        })
        .await?;

    println!("Created tags: {}, {}", tech_tag.name, rust_tag.name);

    let post = tito_db
        .transaction(|tx| {
            let post_model = post_model.clone();
            let post = Post {
                id: DBUuid::new_v4().to_string(),
                title: "Using Rust with TiKV".to_string(),
                content: "Examples of using Rust with TiKV...".to_string(),
                author: "Alice".to_string(),
                tag_ids: vec![tech_tag.id.clone(), rust_tag.id.clone()],
                tags: Vec::new(),
            };
            let post_clone = post.clone();
            async move {
                post_model.build(post_clone, &tx).await?;
                Ok::<_, TitoError>(post)
            }
        })
        .await?;

    println!("Created post: {}", post.title);

    let post_with_tags = post_model
        .find_by_id(&post.id, vec!["tags".to_string()])
        .await?;

    println!("Post: {}", post_with_tags.title);
    println!("Tags:");
    for tag in &post_with_tags.tags {
        println!("- {}", tag.name);
    }

    let mut query = post_model.query_by_index("post-by-author");
    let results = query
        .value("Alice".to_string())
        .relationship("tags")
        .execute()
        .await?;

    println!("\nAlice's posts:");
    for p in &results.items {
        println!("- {} (tags: {})", p.title, p.tags.iter().map(|t| t.name.clone()).collect::<Vec<_>>().join(", "));
    }

    Ok(())
}
