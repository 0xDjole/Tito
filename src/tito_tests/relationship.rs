use super::*;

#[tokio::test]
async fn relationship_stitches_single_and_array_relationships_when_requested() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_tag(&engine, tag("t1", "Tech")).await;
    save_tag(&engine, tag("t2", "Rust")).await;
    save_post(
        &engine,
        post("p1", "a1", vec!["t1".to_string(), "t2".to_string()]),
    )
    .await;

    let found = post_model
        .get("p1")
        .relationship("author")
        .relationship("tags")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.author.unwrap().id, "a1");
    assert_eq!(
        found.tags.into_iter().map(|tag| tag.id).collect::<Vec<_>>(),
        vec!["t1", "t2"]
    );
}

#[tokio::test]
async fn relationship_stitches_nested_array_relationships() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;

    let found = post_model
        .get("p1")
        .relationship("blocks.author")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.blocks[0].author.as_ref().unwrap().id, "a1");
}

#[tokio::test]
async fn relationship_is_not_stitched_when_not_requested() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;

    let found = post_model.get("p1").execute(None).await.unwrap();

    assert!(found.author.is_none());
}

#[tokio::test]
async fn get_many_can_stitch_relationships() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_author(&engine, author("a2", "grace@example.com", 42, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;
    save_post(&engine, post("p2", "a2", vec![])).await;

    let found = post_model
        .get_many(vec!["p2".to_string(), "p1".to_string()])
        .relationship("author")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(
        found
            .iter()
            .map(|post| post.author.as_ref().unwrap().id.clone())
            .collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
}

#[tokio::test]
async fn index_query_can_stitch_relationships() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;

    let mut query = post_model.query_by_index("post-by-author");
    let found = query
        .value("a1")
        .relationship("author")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].author.as_ref().unwrap().id, "a1");
}
