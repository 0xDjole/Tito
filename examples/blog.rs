use serde::{Deserialize, Serialize};
use tito::{
    types::{DBUuid, TitoEngine, TitoModelTrait},
    Guard, TiKV, TitoError, TitoIndex, TitoIndexField, TitoIndexValue,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tag {
    id: String,
    name: String,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Post {
    id: String,
    title: String,
    content: String,
    author: String,
    tag_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PostTitle {
    id: String,
    title: String,
}

impl TitoModelTrait for Tag {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: "tag-by-name",
            fields: vec![TitoIndexField::Text(self.name.clone())],
            value: TitoIndexValue::Full,
            unique: true,
        }]
    }

    fn table() -> String {
        "tag".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

impl TitoModelTrait for Post {
    fn indexes(&self) -> Vec<TitoIndex> {
        let mut indexes = vec![
            TitoIndex {
                name: "post-by-author",
                fields: vec![TitoIndexField::Text(self.author.clone())],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: "post-title-by-author",
                fields: vec![TitoIndexField::Text(self.author.clone())],
                value: TitoIndexValue::Part(&["id", "title"]),
                unique: false,
            },
        ];
        for tag_id in &self.tag_ids {
            indexes.push(TitoIndex {
                name: "post-by-tag",
                fields: vec![TitoIndexField::Text(tag_id.clone())],
                value: TitoIndexValue::KeysOnly,
                unique: false,
            });
        }
        indexes
    }

    fn table() -> String {
        "post".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), TitoError> {
    let tito_db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;
    let posts = tito_db.clone().model::<Post>();
    let tags = tito_db.clone().model::<Tag>();

    let tech = Tag {
        id: DBUuid::new_v4().to_string(),
        name: "Technology".to_string(),
        description: "All about tech".to_string(),
    };
    let rust = Tag {
        id: DBUuid::new_v4().to_string(),
        name: "Rust".to_string(),
        description: "Rust programming".to_string(),
    };
    tito_db
        .transaction(|tx| {
            let tags = tags.clone();
            let tech = tech.clone();
            let rust = rust.clone();
            async move {
                tags.create(&tech, &tx).await?;
                tags.create(&rust, &tx).await
            }
        })
        .await?;
    println!("Created tags: {}, {}", tech.name, rust.name);

    let post = Post {
        id: DBUuid::new_v4().to_string(),
        title: "Using Rust with TiKV".to_string(),
        content: "Examples of using Rust with TiKV...".to_string(),
        author: "Alice".to_string(),
        tag_ids: vec![tech.id.clone(), rust.id.clone()],
    };
    tito_db
        .transaction(|tx| {
            let posts = posts.clone();
            let tags = tags.clone();
            let post = post.clone();
            async move {
                let found = tx
                    .batch_get(tags.read_many(post.tag_ids.clone(), Guard::Share))
                    .await?;
                if found.len() != post.tag_ids.len() {
                    return Err(TitoError::NotFound(
                        "A post names a tag that does not exist".to_string(),
                    ));
                }
                posts.create(&post, &tx).await
            }
        })
        .await?;
    println!("Created post: {}", post.title);

    let holder = tito_db
        .transaction(|tx| {
            let posts = posts.clone();
            let tags = tags.clone();
            let rust_id = rust.id.clone();
            async move {
                let mut used = posts.query_by_index("post-by-tag");
                used.value(TitoIndexField::Text(rust_id.clone()))
                    .limit(Some(1));
                let (holders, tag) = tx
                    .batch_get((used.read_ids(), tags.read(&rust_id, Guard::None)))
                    .await?;
                let holder = holders.items.into_iter().next();
                if holder.is_none() {
                    tags.remove(&tag, &tx).await?;
                }
                Ok::<_, TitoError>(holder)
            }
        })
        .await?;
    if let Some(holder) = holder {
        println!("Tag {} stays: post {} uses it", rust.name, holder);
    }

    let (found, post_tags) = tito_db
        .transaction(|tx| {
            let posts = posts.clone();
            let tags = tags.clone();
            let post_id = post.id.clone();
            async move {
                let post = tx.batch_get(posts.read(&post_id, Guard::None)).await?;
                let tags = tx
                    .batch_get(tags.read_many(post.tag_ids.clone(), Guard::None))
                    .await?;
                Ok::<_, TitoError>((post, tags))
            }
        })
        .await?;
    println!("Post: {} ({})", found.title, found.content);
    println!("Tags:");
    for tag in &post_tags {
        println!("- {} ({})", tag.name, tag.description);
    }

    let titles = posts
        .query_by_index("post-title-by-author")
        .value(TitoIndexField::Text("Alice".to_string()))
        .execute_part::<PostTitle>(None)
        .await?;
    println!("\nAlice's posts:");
    for title in titles.items {
        println!("- {} ({})", title.title, title.id);
    }

    Ok(())
}
