use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::backend::tikv::{TiKVBackend, TiKVTransaction};
use crate::queue::test_engine::{get_raw, keys_with_prefix, native_engine, put_raw};
use crate::types::{TitoEngine, TitoModelConstraints, TitoTransaction};
use crate::{
    encode_index_string, key_after, prefix_end, Batch, BatchAnswers, BatchRequests, Guard, Read,
    TitoError, TitoIndex, TitoIndexField, TitoIndexValue, TitoModel, TitoModelTrait,
    MAX_SAVE_BYTES,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Shop {
    id: String,
    scope: String,
    name: String,
}

impl TitoModelTrait for Shop {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![
            TitoIndex {
                name: "lib-model-shop-by-scope",
                fields: vec![text(&self.scope)],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: "lib-model-shop-name-by-scope",
                fields: vec![text(&self.scope)],
                value: TitoIndexValue::Part(&["id", "name"]),
                unique: false,
            },
        ]
    }

    fn table() -> String {
        "lib-model-shops".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShopName {
    id: String,
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Member {
    id: String,
    scope: String,
    email: String,
    handle: String,
}

impl TitoModelTrait for Member {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![
            TitoIndex {
                name: "lib-model-member-by-email",
                fields: vec![text(&self.scope), text(&self.email)],
                value: TitoIndexValue::Full,
                unique: true,
            },
            TitoIndex {
                name: "lib-model-member-by-handle",
                fields: vec![text(&self.scope), text(&self.handle)],
                value: TitoIndexValue::Full,
                unique: true,
            },
        ]
    }

    fn table() -> String {
        "lib-model-members".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Ledger {
    id: String,
    scope: String,
}

impl TitoModelTrait for Ledger {
    fn indexes(&self) -> Vec<TitoIndex> {
        Vec::new()
    }

    fn table() -> String {
        "lib-model-ledgers".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Debug, PartialEq)]
enum Refusal {
    Closed(String),
    Tito(TitoError),
}

impl From<TitoError> for Refusal {
    fn from(error: TitoError) -> Self {
        Refusal::Tito(error)
    }
}

struct OpenShop {
    note: Read<Shop>,
}

#[async_trait]
impl Batch for OpenShop {
    type Output = Shop;
    type Error = Refusal;

    fn requests(&self, requests: &mut BatchRequests) {
        self.note.requests(requests);
    }

    async fn answer(
        self,
        answers: &mut BatchAnswers,
        tx: &TiKVTransaction,
    ) -> Result<Shop, Refusal> {
        let shop = self.note.answer(answers, tx).await?;
        if shop.name == "Closed" {
            return Err(Refusal::Closed(shop.id));
        }
        Ok(shop)
    }
}

fn fresh_scope() -> String {
    Uuid::new_v4().simple().to_string()
}

fn text(value: &str) -> TitoIndexField {
    TitoIndexField::Text(value.to_string())
}

fn shop(scope: &str, name: &str) -> Shop {
    Shop {
        id: Uuid::new_v4().to_string(),
        scope: scope.to_string(),
        name: name.to_string(),
    }
}

fn member(scope: &str, email: &str, handle: &str) -> Member {
    Member {
        id: Uuid::new_v4().to_string(),
        scope: scope.to_string(),
        email: email.to_string(),
        handle: handle.to_string(),
    }
}

fn json_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("Serialize a test value")
}

fn as_json<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("Serialize a test value")
}

fn scope_entry(scope: &str, id: &str) -> String {
    format!(
        "index:lib-model-shop-by-scope:{}:{}:",
        encode_index_string(scope),
        encode_index_string(id)
    )
}

fn name_entry(scope: &str, id: &str) -> String {
    format!(
        "index:lib-model-shop-name-by-scope:{}:{}:",
        encode_index_string(scope),
        encode_index_string(id)
    )
}

fn scope_entries(scope: &str) -> String {
    format!(
        "index:lib-model-shop-by-scope:{}:",
        encode_index_string(scope)
    )
}

fn name_entries(scope: &str) -> String {
    format!(
        "index:lib-model-shop-name-by-scope:{}:",
        encode_index_string(scope)
    )
}

fn unread(key: &str) -> String {
    format!("Record '{key}' was not read in this transaction")
}

fn fence_keys(table: &str, id: &str) -> Vec<String> {
    let base = format!(
        "fence:{}:{}",
        encode_index_string(table),
        encode_index_string(id)
    );
    let mut keys = (0..16)
        .map(|number| format!("{base}:{number}"))
        .collect::<Vec<_>>();
    keys.push(format!("{base}:exclusive"));
    keys.sort();
    keys
}

fn exclusive_key(table: &str, id: &str) -> String {
    format!(
        "fence:{}:{}:exclusive",
        encode_index_string(table),
        encode_index_string(id)
    )
}

fn deleted(all: &[String], left: &[String]) -> Vec<String> {
    all.iter()
        .filter(|key| !left.contains(*key))
        .cloned()
        .collect()
}

fn shared_number(key: &str) -> u16 {
    key.rsplit(':')
        .next()
        .and_then(|number| number.parse::<u16>().ok())
        .filter(|number| *number < 16)
        .expect("A shared fence key ends with a number below 16")
}

async fn fences_left(engine: &TiKVBackend, table: &str, id: &str) -> Vec<String> {
    let prefix = format!(
        "fence:{}:{}:",
        encode_index_string(table),
        encode_index_string(id)
    );
    keys_with_prefix(engine, &prefix).await
}

async fn put_keys(engine: &TiKVBackend, keys: &[String]) {
    engine
        .transaction(|tx| async move {
            for key in keys {
                tx.put(key.as_bytes(), key.as_bytes()).await?;
            }
            Ok::<_, TitoError>(())
        })
        .await
        .expect("Write the raw test keys");
}

async fn delete_keys(engine: &TiKVBackend, keys: &[String]) {
    engine
        .transaction(|tx| async move {
            for key in keys {
                tx.delete(key.as_bytes()).await?;
            }
            Ok::<_, TitoError>(())
        })
        .await
        .expect("Delete the raw test keys");
}

async fn stored_value(engine: &TiKVBackend, key: &str) -> Option<Value> {
    get_raw(engine, key)
        .await
        .map(|bytes| serde_json::from_slice(&bytes).expect("A stored row holds JSON"))
}

async fn create_one<T: TitoModelConstraints>(
    model: &TitoModel<TiKVBackend, T>,
    record: &T,
) -> Result<(), TitoError> {
    model
        .engine
        .transaction(|tx| async move { model.create(record, &tx).await })
        .await
}

async fn read_one<T: TitoModelConstraints>(
    model: &TitoModel<TiKVBackend, T>,
    id: &str,
) -> Result<Option<T>, TitoError> {
    model
        .engine
        .transaction(|tx| async move { tx.batch_get(model.read_maybe(id, Guard::None)).await })
        .await
}

async fn update_one<T: TitoModelConstraints>(
    model: &TitoModel<TiKVBackend, T>,
    new: &T,
) -> Result<(), TitoError> {
    model
        .engine
        .transaction(|tx| async move {
            let old = tx.batch_get(model.read(&new.id(), Guard::None)).await?;
            model.update(&old, new, &tx).await
        })
        .await
}

async fn remove_one<T: TitoModelConstraints>(
    model: &TitoModel<TiKVBackend, T>,
    id: &str,
) -> Result<(), TitoError> {
    model
        .engine
        .transaction(|tx| async move {
            let stored = tx.batch_get(model.read(id, Guard::None)).await?;
            model.remove(&stored, &tx).await
        })
        .await
}

#[tokio::test]
async fn a_record_is_created_read_updated_and_removed_with_its_entries() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let created = &shop(scope, "Corner");
    let key = &shops.key(&created.id);
    assert_eq!(shops.get_table(), "table:lib-model-shops");
    assert_eq!(*key, format!("table:lib-model-shops:{}", created.id));

    create_one(shops, created).await.unwrap();
    assert_eq!(stored_value(engine, key).await, Some(as_json(created)));
    assert_eq!(
        stored_value(engine, &scope_entry(scope, &created.id)).await,
        Some(as_json(created))
    );
    assert_eq!(
        stored_value(engine, &name_entry(scope, &created.id)).await,
        Some(json!([created.id, {"id": created.id, "name": "Corner"}]))
    );

    let found = engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read(&created.id, Guard::None),
                shops.read_maybe(&created.id, Guard::None),
            ))
            .await
        })
        .await
        .unwrap();
    assert_eq!(found, (created.clone(), Some(created.clone())));

    let renamed = &Shop {
        name: "Renamed".to_string(),
        ..created.clone()
    };
    update_one(shops, renamed).await.unwrap();
    assert_eq!(stored_value(engine, key).await, Some(as_json(renamed)));
    assert_eq!(
        stored_value(engine, &scope_entry(scope, &created.id)).await,
        Some(as_json(renamed))
    );
    assert_eq!(
        stored_value(engine, &name_entry(scope, &created.id)).await,
        Some(json!([created.id, {"id": created.id, "name": "Renamed"}]))
    );

    remove_one(shops, &created.id).await.unwrap();
    assert_eq!(get_raw(engine, key).await, None);
    assert!(keys_with_prefix(engine, &scope_entries(scope))
        .await
        .is_empty());
    assert!(keys_with_prefix(engine, &name_entries(scope))
        .await
        .is_empty());
    assert_eq!(read_one(shops, &created.id).await.unwrap(), None);
    let missing = engine
        .transaction(|tx| async move { tx.batch_get(shops.read(&created.id, Guard::None)).await })
        .await
        .unwrap_err();
    assert_eq!(
        missing,
        TitoError::NotFound(format!("Record '{key}' not found in database"))
    );
}

#[tokio::test]
async fn read_many_returns_the_records_it_found_in_the_order_asked() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let a = &shop(scope, "A");
    let b = &shop(scope, "B");
    let c = &shop(scope, "C");
    engine
        .transaction(|tx| async move {
            shops.create(a, &tx).await?;
            shops.create(b, &tx).await?;
            shops.create(c, &tx).await
        })
        .await
        .unwrap();

    let missing = &Uuid::new_v4().to_string();
    let (found, none_found, none_asked) = engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read_many(
                    vec![c.id.clone(), missing.clone(), a.id.clone(), b.id.clone()],
                    Guard::None,
                ),
                shops.read_many(vec![missing.clone()], Guard::Share),
                shops.read_many(Vec::new(), Guard::None),
            ))
            .await
        })
        .await
        .unwrap();
    assert_eq!(found, vec![c.clone(), a.clone(), b.clone()]);
    assert!(none_found.is_empty());
    assert!(none_asked.is_empty());
}

#[tokio::test]
async fn a_create_of_an_id_already_stored_is_refused_at_commit_and_not_retried() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let first = &shop(scope, "First");
    create_one(shops, first).await.unwrap();

    let second = &Shop {
        name: "Second".to_string(),
        ..first.clone()
    };
    let attempts = &AtomicUsize::new(0);
    let refused = engine
        .transaction(|tx| async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            shops.create(second, &tx).await
        })
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        TitoError::RecordExists {
            model: "lib-model-shops".to_string(),
            id: first.id.clone(),
        }
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        stored_value(engine, &shops.key(&first.id)).await,
        Some(as_json(first))
    );
    assert_eq!(
        stored_value(engine, &scope_entry(scope, &first.id)).await,
        Some(as_json(first))
    );
    assert_eq!(
        stored_value(engine, &name_entry(scope, &first.id)).await,
        Some(json!([first.id, {"id": first.id, "name": "First"}]))
    );
}

#[tokio::test]
async fn a_create_of_an_id_its_transaction_already_holds_is_refused_at_once_and_ends_the_transaction(
) {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let first = &shop(&fresh_scope(), "First");
    let again = &Shop {
        name: "Again".to_string(),
        ..first.clone()
    };
    let exists = TitoError::RecordExists {
        model: "lib-model-shops".to_string(),
        id: first.id.clone(),
    };

    let tx = engine.begin_transaction().await.unwrap();
    shops.create(first, &tx).await.unwrap();
    assert_eq!(shops.create(again, &tx).await, Err(exists.clone()));
    assert_eq!(tx.commit().await, Err(exists.clone()));
    assert_eq!(read_one(shops, &first.id).await.unwrap(), None);

    let created_twice = engine
        .transaction(|tx| async move {
            shops.create(first, &tx).await?;
            Ok::<_, TitoError>(shops.create(again, &tx).await)
        })
        .await;
    assert_eq!(created_twice, Err(exists.clone()));
    assert_eq!(read_one(shops, &first.id).await.unwrap(), None);

    create_one(shops, first).await.unwrap();
    let tx = engine.begin_transaction().await.unwrap();
    assert_eq!(
        tx.batch_get(shops.read_maybe(&first.id, Guard::None)).await,
        Ok(Some(first.clone()))
    );
    assert_eq!(shops.create(again, &tx).await, Err(exists.clone()));
    assert_eq!(tx.commit().await, Err(exists));
    assert_eq!(
        read_one(shops, &first.id).await.unwrap(),
        Some(first.clone())
    );
}

#[tokio::test]
async fn update_and_remove_refuse_a_record_this_transaction_has_not_read() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let stored = &shop(&fresh_scope(), "Stored");
    create_one(shops, stored).await.unwrap();
    let renamed = &Shop {
        name: "Renamed".to_string(),
        ..stored.clone()
    };

    let (updated, removed) = engine
        .transaction(|tx| async move {
            Ok::<_, TitoError>((
                shops.update(stored, renamed, &tx).await,
                shops.remove(stored, &tx).await,
            ))
        })
        .await
        .unwrap();
    let key = shops.key(&stored.id);
    assert_eq!(updated, Err(TitoError::UpdateFailed(unread(&key))));
    assert_eq!(removed, Err(TitoError::DeleteFailed(unread(&key))));
    assert_eq!(
        read_one(shops, &stored.id).await.unwrap(),
        Some(stored.clone())
    );
}

#[tokio::test]
async fn update_and_remove_refuse_a_changed_id_and_a_removed_record() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let stored = &shop(scope, "Stored");
    create_one(shops, stored).await.unwrap();

    let (moved, after_remove) = engine
        .transaction(|tx| async move {
            let old = tx.batch_get(shops.read(&stored.id, Guard::None)).await?;
            let first = Shop {
                name: "First".to_string(),
                ..old.clone()
            };
            let second = Shop {
                name: "Second".to_string(),
                ..old.clone()
            };
            let elsewhere = Shop {
                id: Uuid::new_v4().to_string(),
                ..first.clone()
            };
            shops.update(&old, &first, &tx).await?;
            shops.update(&old, &second, &tx).await?;
            let moved = shops.update(&second, &elsewhere, &tx).await;
            shops.remove(&old, &tx).await?;
            let after_remove = vec![
                shops.update(&second, &first, &tx).await,
                shops.remove(&second, &tx).await,
            ];
            Ok::<_, TitoError>((moved, after_remove))
        })
        .await
        .unwrap();
    let key = shops.key(&stored.id);
    assert_eq!(
        moved,
        Err(TitoError::UpdateFailed(format!(
            "An update of record '{key}' must keep its id"
        )))
    );
    assert_eq!(
        after_remove,
        vec![
            Err(TitoError::UpdateFailed(unread(&key))),
            Err(TitoError::DeleteFailed(unread(&key))),
        ]
    );
    assert_eq!(get_raw(engine, &key).await, None);
    assert!(keys_with_prefix(engine, &scope_entries(scope))
        .await
        .is_empty());
    assert!(keys_with_prefix(engine, &name_entries(scope))
        .await
        .is_empty());
}

#[tokio::test]
async fn an_update_replaces_the_entries_its_transaction_read_whatever_the_copy_handed_over_says() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let misstated_scope = &fresh_scope();
    let new_scope = &fresh_scope();
    let named = &shop(scope, "Stored");
    let scoped = &shop(scope, "Scoped");
    let ada = &member(scope, "ada@example.com", "ada");
    engine
        .transaction(|tx| async move {
            shops.create(named, &tx).await?;
            shops.create(scoped, &tx).await?;
            members.create(ada, &tx).await
        })
        .await
        .unwrap();
    let renamed = &Shop {
        name: "Renamed".to_string(),
        ..named.clone()
    };
    let rescoped = &Shop {
        scope: new_scope.clone(),
        ..scoped.clone()
    };
    let lovelace = &Member {
        handle: "lovelace".to_string(),
        ..ada.clone()
    };

    engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read(&named.id, Guard::None),
                shops.read(&scoped.id, Guard::None),
                members.read(&ada.id, Guard::None),
            ))
            .await?;
            shops.update(renamed, renamed, &tx).await?;
            let misstated = Shop {
                scope: misstated_scope.clone(),
                ..scoped.clone()
            };
            shops.update(&misstated, rescoped, &tx).await?;
            let misstated = Member {
                handle: "misstated".to_string(),
                ..ada.clone()
            };
            members.update(&misstated, lovelace, &tx).await
        })
        .await
        .unwrap();

    assert_eq!(
        stored_value(engine, &shops.key(&named.id)).await,
        Some(as_json(renamed))
    );
    assert_eq!(
        stored_value(engine, &scope_entry(scope, &named.id)).await,
        Some(as_json(renamed))
    );
    assert_eq!(
        stored_value(engine, &name_entry(scope, &named.id)).await,
        Some(json!([named.id, {"id": named.id, "name": "Renamed"}]))
    );
    assert_eq!(
        keys_with_prefix(engine, &scope_entries(scope)).await,
        vec![scope_entry(scope, &named.id)]
    );
    assert_eq!(
        keys_with_prefix(engine, &name_entries(scope)).await,
        vec![name_entry(scope, &named.id)]
    );
    assert!(keys_with_prefix(engine, &scope_entries(misstated_scope))
        .await
        .is_empty());
    assert!(keys_with_prefix(engine, &name_entries(misstated_scope))
        .await
        .is_empty());
    assert_eq!(
        keys_with_prefix(engine, &scope_entries(new_scope)).await,
        vec![scope_entry(new_scope, &scoped.id)]
    );
    assert_eq!(
        stored_value(engine, &scope_entry(new_scope, &scoped.id)).await,
        Some(as_json(rescoped))
    );
    assert_eq!(
        stored_value(engine, &name_entry(new_scope, &scoped.id)).await,
        Some(json!([scoped.id, {"id": scoped.id, "name": "Scoped"}]))
    );

    let handles = format!("index:lib-model-member-by-handle:{scope}:");
    assert_eq!(
        keys_with_prefix(engine, &handles).await,
        vec![format!("{handles}lovelace:")]
    );
    assert_eq!(
        stored_value(engine, &format!("{handles}lovelace:")).await,
        Some(as_json(lovelace))
    );
    assert_eq!(
        stored_value(
            engine,
            &format!("index:lib-model-member-by-email:{scope}:ada@example.com:")
        )
        .await,
        Some(as_json(lovelace))
    );
    assert_eq!(
        stored_value(engine, &members.key(&ada.id)).await,
        Some(as_json(lovelace))
    );
}

#[tokio::test]
async fn a_remove_deletes_the_entries_its_transaction_read_whatever_the_copy_handed_over_says() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let misstated_scope = &fresh_scope();
    let other_scope = &fresh_scope();
    let stored = &shop(scope, "Stored");
    let bystander = &shop(misstated_scope, "Bystander");
    let ada = &member(scope, "ada@example.com", "ada");
    engine
        .transaction(|tx| async move {
            shops.create(stored, &tx).await?;
            shops.create(bystander, &tx).await?;
            members.create(ada, &tx).await
        })
        .await
        .unwrap();

    engine
        .transaction(|tx| async move {
            let (read_shop, read_member) = tx
                .batch_get((
                    shops.read(&stored.id, Guard::None),
                    members.read(&ada.id, Guard::None),
                ))
                .await?;
            let misstated = Shop {
                scope: misstated_scope.clone(),
                name: "Misstated".to_string(),
                ..read_shop
            };
            shops.remove(&misstated, &tx).await?;
            let misstated = Member {
                email: "misstated@example.com".to_string(),
                handle: "misstated".to_string(),
                scope: other_scope.clone(),
                ..read_member
            };
            members.remove(&misstated, &tx).await
        })
        .await
        .unwrap();

    assert_eq!(get_raw(engine, &shops.key(&stored.id)).await, None);
    assert!(keys_with_prefix(engine, &scope_entries(scope))
        .await
        .is_empty());
    assert!(keys_with_prefix(engine, &name_entries(scope))
        .await
        .is_empty());
    assert_eq!(
        keys_with_prefix(engine, &scope_entries(misstated_scope)).await,
        vec![scope_entry(misstated_scope, &bystander.id)]
    );
    assert_eq!(
        read_one(shops, &bystander.id).await.unwrap(),
        Some(bystander.clone())
    );
    assert_eq!(get_raw(engine, &members.key(&ada.id)).await, None);
    assert!(
        keys_with_prefix(engine, &format!("index:lib-model-member-by-email:{scope}:"))
            .await
            .is_empty()
    );
    assert!(keys_with_prefix(
        engine,
        &format!("index:lib-model-member-by-handle:{scope}:")
    )
    .await
    .is_empty());
}

#[tokio::test]
async fn a_record_created_in_a_transaction_can_be_updated_or_removed_there() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let draft = &shop(scope, "Draft");
    let published = &Shop {
        name: "Published".to_string(),
        ..draft.clone()
    };
    let dropped = &shop(scope, "Dropped");
    engine
        .transaction(|tx| async move {
            shops.create(draft, &tx).await?;
            shops.update(draft, published, &tx).await?;
            shops.create(dropped, &tx).await?;
            shops.remove(dropped, &tx).await
        })
        .await
        .unwrap();

    assert_eq!(
        stored_value(engine, &shops.key(&draft.id)).await,
        Some(as_json(published))
    );
    assert_eq!(
        stored_value(engine, &scope_entry(scope, &draft.id)).await,
        Some(as_json(published))
    );
    assert_eq!(
        stored_value(engine, &name_entry(scope, &draft.id)).await,
        Some(json!([draft.id, {"id": draft.id, "name": "Published"}]))
    );
    assert_eq!(get_raw(engine, &shops.key(&dropped.id)).await, None);
    assert_eq!(
        keys_with_prefix(engine, &scope_entries(scope)).await,
        vec![scope_entry(scope, &draft.id)]
    );
    assert_eq!(
        keys_with_prefix(engine, &name_entries(scope)).await,
        vec![name_entry(scope, &draft.id)]
    );
}

#[tokio::test]
async fn a_part_or_an_ids_read_does_not_count_as_a_read_of_the_record() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let stored = &shop(scope, "Stored");
    create_one(shops, stored).await.unwrap();
    let renamed = &Shop {
        name: "Renamed".to_string(),
        ..stored.clone()
    };

    let (names, ids, after_lists, records, after_page) = engine
        .transaction(|tx| async move {
            let names = shops
                .query_by_index("lib-model-shop-name-by-scope")
                .value(text(scope))
                .execute_part::<ShopName>(Some(&tx))
                .await?;
            let ids = shops
                .query_by_index("lib-model-shop-by-scope")
                .value(text(scope))
                .execute_ids(Some(&tx))
                .await?;
            let after_lists = shops.update(stored, renamed, &tx).await;
            let records = shops
                .query_by_index("lib-model-shop-by-scope")
                .value(text(scope))
                .execute(Some(&tx))
                .await?;
            let after_page = shops.update(stored, renamed, &tx).await;
            Ok::<_, TitoError>((
                names.items,
                ids.items,
                after_lists,
                records.items,
                after_page,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        names,
        vec![ShopName {
            id: stored.id.clone(),
            name: "Stored".to_string(),
        }]
    );
    assert_eq!(ids, vec![stored.id.clone()]);
    assert_eq!(
        after_lists,
        Err(TitoError::UpdateFailed(unread(&shops.key(&stored.id))))
    );
    assert_eq!(records, vec![stored.clone()]);
    assert_eq!(after_page, Ok(()));
    assert_eq!(
        read_one(shops, &stored.id).await.unwrap(),
        Some(renamed.clone())
    );
}

#[tokio::test]
async fn batch_get_answers_every_element_of_a_tuple_in_the_order_given() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let a = &shop(scope, "A");
    let b = &shop(scope, "B");
    let closed = &shop(&fresh_scope(), "Closed");
    let ada = &member(scope, "ada@example.com", "ada");
    engine
        .transaction(|tx| async move {
            shops.create(a, &tx).await?;
            shops.create(b, &tx).await?;
            shops.create(closed, &tx).await?;
            members.create(ada, &tx).await
        })
        .await
        .unwrap();
    let mut by_id = vec![a.clone(), b.clone()];
    by_id.sort_by(|left, right| left.id.cmp(&right.id));
    let ids = by_id.iter().map(|shop| shop.id.clone()).collect::<Vec<_>>();
    let names = by_id
        .iter()
        .map(|shop| ShopName {
            id: shop.id.clone(),
            name: shop.name.clone(),
        })
        .collect::<Vec<_>>();
    let mut listing = shops.query_by_index("lib-model-shop-by-scope");
    listing.value(text(scope));
    let ids_note = &listing.read_ids();
    let records_note = &listing.read();
    let mut naming = shops.query_by_index("lib-model-shop-name-by-scope");
    naming.value(text(scope));
    let names_note = &naming.read_part::<ShopName>();

    let (first, listed) = engine
        .transaction(|tx| async move {
            tx.batch_get((shops.read(&a.id, Guard::None), ids_note.clone()))
                .await
        })
        .await
        .unwrap();
    assert_eq!(first, a.clone());
    assert_eq!(listed.items, ids);
    assert_eq!(listed.cursor, None);

    let missing = &Uuid::new_v4().to_string();
    let (got_b, nothing, many, by_email, no_handle, listed, records, parts) = engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read(&b.id, Guard::None),
                shops.read_maybe(missing, Guard::None),
                shops.read_many(vec![b.id.clone(), a.id.clone()], Guard::None),
                members.read_unique(
                    "lib-model-member-by-email",
                    vec![text(scope), text("ada@example.com")],
                    Guard::None,
                ),
                members.read_unique_maybe(
                    "lib-model-member-by-handle",
                    vec![text(scope), text("nobody")],
                    Guard::None,
                ),
                ids_note.clone(),
                records_note.clone(),
                names_note.clone(),
            ))
            .await
        })
        .await
        .unwrap();
    assert_eq!(got_b, b.clone());
    assert_eq!(nothing, None);
    assert_eq!(many, vec![b.clone(), a.clone()]);
    assert_eq!(by_email, ada.clone());
    assert_eq!(no_handle, None);
    assert_eq!(listed.items, ids);
    assert_eq!(records.items, by_id);
    assert_eq!(records.cursor, None);
    assert_eq!(parts.items, names);

    let (open, rest) = engine
        .transaction(|tx| async move {
            tx.batch_get((
                OpenShop {
                    note: shops.read(&a.id, Guard::None),
                },
                shops.read_many(vec![b.id.clone()], Guard::None),
            ))
            .await
        })
        .await
        .unwrap();
    assert_eq!(open, a.clone());
    assert_eq!(rest, vec![b.clone()]);

    let refused = engine
        .transaction(|tx| async move {
            tx.batch_get((
                OpenShop {
                    note: shops.read(&closed.id, Guard::None),
                },
                shops.read(&a.id, Guard::None),
            ))
            .await
        })
        .await
        .unwrap_err();
    assert_eq!(refused, Refusal::Closed(closed.id.clone()));

    let converted = engine
        .transaction(|tx| async move {
            tx.batch_get((
                OpenShop {
                    note: shops.read(&a.id, Guard::None),
                },
                shops.read(missing, Guard::None),
            ))
            .await
        })
        .await
        .unwrap_err();
    assert_eq!(
        converted,
        Refusal::Tito(TitoError::NotFound(format!(
            "Record '{}' not found in database",
            shops.key(missing)
        )))
    );
}

#[tokio::test]
async fn batch_get_answers_a_vec_of_notes_in_the_order_given() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let a = &shop(scope, "A");
    let b = &shop(scope, "B");
    let ada = &member(scope, "ada@example.com", "ada");
    let grace = &member(scope, "grace@example.com", "grace");
    let alan = &member(scope, "alan@example.com", "alan");
    engine
        .transaction(|tx| async move {
            shops.create(a, &tx).await?;
            shops.create(b, &tx).await?;
            members.create(ada, &tx).await?;
            members.create(grace, &tx).await?;
            members.create(alan, &tx).await
        })
        .await
        .unwrap();
    let missing = &Uuid::new_v4().to_string();

    let (by_handle, by_id, nothing, nested) = engine
        .transaction(|tx| async move {
            let by_handle = tx
                .batch_get(
                    ["grace", "alan", "ada", "grace"]
                        .iter()
                        .map(|handle| {
                            members.read_unique(
                                "lib-model-member-by-handle",
                                vec![text(scope), text(handle)],
                                Guard::None,
                            )
                        })
                        .collect::<Vec<_>>(),
                )
                .await?;
            let by_id = tx
                .batch_get(
                    [&b.id, missing, &a.id]
                        .iter()
                        .map(|id| shops.read_maybe(id, Guard::None))
                        .collect::<Vec<_>>(),
                )
                .await?;
            let nothing = tx.batch_get(Vec::<Read<Shop>>::new()).await?;
            let nested = tx
                .batch_get((
                    shops.read(&a.id, Guard::None),
                    ["alan@example.com", "nobody@example.com"]
                        .iter()
                        .map(|email| {
                            members.read_unique_maybe(
                                "lib-model-member-by-email",
                                vec![text(scope), text(email)],
                                Guard::None,
                            )
                        })
                        .collect::<Vec<_>>(),
                    Vec::<Read<Option<Member>>>::new(),
                ))
                .await?;
            Ok::<_, TitoError>((by_handle, by_id, nothing, nested))
        })
        .await
        .unwrap();
    assert_eq!(
        by_handle,
        vec![grace.clone(), alan.clone(), ada.clone(), grace.clone()]
    );
    assert_eq!(by_id, vec![Some(b.clone()), None, Some(a.clone())]);
    assert!(nothing.is_empty());
    assert_eq!(
        nested,
        (a.clone(), vec![Some(alan.clone()), None], Vec::new())
    );

    let refused = engine
        .transaction(|tx| async move {
            tx.batch_get(
                [&a.id, missing, &b.id]
                    .iter()
                    .map(|id| shops.read(id, Guard::None))
                    .collect::<Vec<_>>(),
            )
            .await
        })
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        TitoError::NotFound(format!(
            "Record '{}' not found in database",
            shops.key(missing)
        ))
    );
}

#[tokio::test]
async fn a_mapped_read_converts_its_answer_and_a_failed_conversion_fails_the_read() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let stored = &shop(&fresh_scope(), "Stored");
    create_one(shops, stored).await.unwrap();
    let renamed = &Shop {
        name: "Renamed".to_string(),
        ..stored.clone()
    };
    let missing = &Uuid::new_v4().to_string();

    let (refused, unread_update) = engine
        .transaction(|tx| async move {
            let refused = tx
                .batch_get(shops.read(&stored.id, Guard::None).map(|_| {
                    Err::<String, _>(TitoError::DeserializationFailed("Not a shop".to_string()))
                }))
                .await;
            Ok::<_, TitoError>((refused, shops.update(stored, renamed, &tx).await))
        })
        .await
        .unwrap();
    assert_eq!(
        refused,
        Err(TitoError::DeserializationFailed("Not a shop".to_string()))
    );
    assert_eq!(
        unread_update,
        Err(TitoError::UpdateFailed(unread(&shops.key(&stored.id))))
    );

    let (name, no_name) = engine
        .transaction(|tx| async move {
            let names = tx
                .batch_get((
                    shops
                        .read(&stored.id, Guard::None)
                        .map(|shop| Ok(shop.name)),
                    shops
                        .read_maybe(missing, Guard::None)
                        .map(|shop| Ok(shop.map(|shop| shop.name))),
                ))
                .await?;
            shops.update(stored, renamed, &tx).await?;
            Ok::<_, TitoError>(names)
        })
        .await
        .unwrap();
    assert_eq!(name, "Stored");
    assert_eq!(no_name, None);
    assert_eq!(
        read_one(shops, &stored.id).await.unwrap(),
        Some(renamed.clone())
    );
}

#[tokio::test]
async fn a_stored_record_that_does_not_decode_fails_every_read_of_it() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let good = &shop(&fresh_scope(), "Good");
    create_one(shops, good).await.unwrap();
    let malformed = &Uuid::new_v4().to_string();
    let foreign = &Uuid::new_v4().to_string();
    let partial = &Uuid::new_v4().to_string();
    let corrupt = vec![shops.key(malformed), shops.key(foreign), shops.key(partial)];
    put_raw(engine, &corrupt[0], b"{".to_vec()).await;
    put_raw(engine, &corrupt[1], json_bytes(good)).await;
    put_raw(engine, &corrupt[2], json_bytes(&json!({ "id": partial }))).await;

    let outcomes = engine
        .transaction(|tx| async move {
            Ok::<_, TitoError>(vec![
                tx.batch_get(shops.read(malformed, Guard::None))
                    .await
                    .map(|_| ()),
                tx.batch_get(shops.read_maybe(foreign, Guard::None))
                    .await
                    .map(|_| ()),
                tx.batch_get(shops.read(partial, Guard::Share))
                    .await
                    .map(|_| ()),
                tx.batch_get(
                    shops.read_many(vec![good.id.clone(), malformed.clone()], Guard::None),
                )
                .await
                .map(|_| ()),
            ])
        })
        .await
        .unwrap();
    delete_keys(engine, &corrupt).await;

    let failed_to_decode = |outcome: &Result<(), TitoError>, key: &str| {
        matches!(
            outcome,
            Err(TitoError::DeserializationFailed(message))
                if message.starts_with(&format!("Failed to deserialize record '{key}': "))
        )
    };
    assert!(
        failed_to_decode(&outcomes[0], &corrupt[0]),
        "{:?}",
        outcomes[0]
    );
    assert_eq!(
        outcomes[1],
        Err(TitoError::DeserializationFailed(format!(
            "Record '{}' holds the id of another record",
            corrupt[1]
        )))
    );
    assert!(
        failed_to_decode(&outcomes[2], &corrupt[2]),
        "{:?}",
        outcomes[2]
    );
    assert!(
        failed_to_decode(&outcomes[3], &corrupt[0]),
        "{:?}",
        outcomes[3]
    );
}

#[tokio::test]
async fn create_update_and_remove_each_delete_the_seventeen_escaped_fence_keys() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let odd = &Shop {
        id: format!("{scope}:a\\b"),
        scope: scope.clone(),
        name: "Odd".to_string(),
    };
    let fences = &fence_keys("lib-model-shops", &odd.id);
    assert_eq!(fences.len(), 17);
    assert!(fences.contains(&format!("fence:lib-model-shops:{scope}\\:a\\\\b:7")));
    assert_eq!(
        fences.last(),
        Some(&format!("fence:lib-model-shops:{scope}\\:a\\\\b:exclusive"))
    );

    put_keys(engine, fences).await;
    create_one(shops, odd).await.unwrap();
    assert!(fences_left(engine, "lib-model-shops", &odd.id)
        .await
        .is_empty());
    let ids = shops
        .query_by_index("lib-model-shop-by-scope")
        .value(text(scope))
        .execute_ids(None)
        .await
        .unwrap();
    assert_eq!(ids.items, vec![odd.id.clone()]);
    let names = shops
        .query_by_index("lib-model-shop-name-by-scope")
        .value(text(scope))
        .execute_part::<ShopName>(None)
        .await
        .unwrap();
    assert_eq!(
        names.items,
        vec![ShopName {
            id: odd.id.clone(),
            name: "Odd".to_string(),
        }]
    );

    put_keys(engine, fences).await;
    update_one(
        shops,
        &Shop {
            name: "Renamed".to_string(),
            ..odd.clone()
        },
    )
    .await
    .unwrap();
    assert!(fences_left(engine, "lib-model-shops", &odd.id)
        .await
        .is_empty());

    put_keys(engine, fences).await;
    remove_one(shops, &odd.id).await.unwrap();
    assert!(fences_left(engine, "lib-model-shops", &odd.id)
        .await
        .is_empty());
    assert_eq!(get_raw(engine, &shops.key(&odd.id)).await, None);
}

#[tokio::test]
async fn a_read_for_share_in_a_writing_transaction_deletes_one_shared_fence_key_of_each_record_found(
) {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let a = &shop(scope, "A");
    let b = &shop(scope, "B");
    let ada = &member(scope, "ada@example.com", "ada");
    engine
        .transaction(|tx| async move {
            shops.create(a, &tx).await?;
            shops.create(b, &tx).await?;
            members.create(ada, &tx).await
        })
        .await
        .unwrap();
    let guarded = [
        ("lib-model-shops", a.id.clone()),
        ("lib-model-shops", b.id.clone()),
        ("lib-model-members", ada.id.clone()),
    ];
    let all = guarded
        .iter()
        .flat_map(|(table, id)| fence_keys(table, id))
        .collect::<Vec<_>>();
    put_keys(engine, &all).await;

    let missing = &Uuid::new_v4().to_string();
    let scratch = &format!("lib-model-scratch:{scope}");
    engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read(&a.id, Guard::Share),
                shops.read_many(vec![b.id.clone(), missing.clone()], Guard::Share),
                members.read_unique(
                    "lib-model-member-by-email",
                    vec![text(scope), text("ada@example.com")],
                    Guard::Share,
                ),
                shops.read_maybe(missing, Guard::Share),
            ))
            .await?;
            tx.put(scratch.as_bytes(), b"written").await
        })
        .await
        .unwrap();

    let mut numbers = Vec::new();
    for (table, id) in &guarded {
        let fences = fence_keys(table, id);
        let gone = deleted(&fences, &fences_left(engine, table, id).await);
        assert_eq!(gone.len(), 1, "{gone:?}");
        numbers.push(shared_number(&gone[0]));
    }
    assert!(
        numbers.iter().all(|number| *number == numbers[0]),
        "{numbers:?}"
    );
}

#[tokio::test]
async fn a_read_for_update_deletes_the_exclusive_fence_key_even_when_its_transaction_writes_nothing(
) {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let a = &shop(scope, "A");
    let b = &shop(scope, "B");
    engine
        .transaction(|tx| async move {
            shops.create(a, &tx).await?;
            shops.create(b, &tx).await
        })
        .await
        .unwrap();
    let a_fences = fence_keys("lib-model-shops", &a.id);
    let b_fences = fence_keys("lib-model-shops", &b.id);
    put_keys(engine, &[a_fences.clone(), b_fences.clone()].concat()).await;

    engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read(&a.id, Guard::Share),
                shops.read(&a.id, Guard::Update),
                shops.read(&b.id, Guard::Share),
            ))
            .await?;
            Ok::<_, TitoError>(())
        })
        .await
        .unwrap();

    assert_eq!(
        deleted(
            &a_fences,
            &fences_left(engine, "lib-model-shops", &a.id).await
        ),
        vec![exclusive_key("lib-model-shops", &a.id)]
    );
    let b_gone = deleted(
        &b_fences,
        &fences_left(engine, "lib-model-shops", &b.id).await,
    );
    assert_eq!(b_gone.len(), 1, "{b_gone:?}");
    assert!(shared_number(&b_gone[0]) < 16);
}

#[tokio::test]
async fn plain_reads_index_pages_and_a_transaction_that_only_reads_for_share_delete_no_fence_key() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let a = &shop(scope, "A");
    create_one(shops, a).await.unwrap();
    let fences = fence_keys("lib-model-shops", &a.id);
    put_keys(engine, &fences).await;
    let scratch = &format!("lib-model-scratch:{scope}");

    engine
        .transaction(|tx| async move {
            let mut listing = shops.query_by_index("lib-model-shop-by-scope");
            listing.value(text(scope));
            tx.batch_get((
                shops.read(&a.id, Guard::None),
                shops.read_many(vec![a.id.clone()], Guard::None),
                listing.read(),
                listing.read_ids(),
            ))
            .await?;
            listing.execute(Some(&tx)).await?;
            tx.put(scratch.as_bytes(), b"written").await
        })
        .await
        .unwrap();
    assert_eq!(fences_left(engine, "lib-model-shops", &a.id).await, fences);

    engine
        .transaction(|tx| async move {
            tx.batch_get((
                shops.read(&a.id, Guard::Share),
                shops.read_many(vec![a.id.clone()], Guard::Share),
            ))
            .await?;
            Ok::<_, TitoError>(())
        })
        .await
        .unwrap();
    assert_eq!(fences_left(engine, "lib-model-shops", &a.id).await, fences);
}

#[tokio::test]
async fn a_failed_closure_leaves_records_entries_and_fence_keys_as_they_were() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let stored = &shop(scope, "Stored");
    create_one(shops, stored).await.unwrap();
    let fences = fence_keys("lib-model-shops", &stored.id);
    put_keys(engine, &fences).await;
    let renamed = &Shop {
        name: "Renamed".to_string(),
        ..stored.clone()
    };
    let added = &shop(scope, "Added");

    let failed = engine
        .transaction(|tx| async move {
            let old = tx.batch_get(shops.read(&stored.id, Guard::Update)).await?;
            shops.update(&old, renamed, &tx).await?;
            shops.create(added, &tx).await?;
            Err::<(), _>(TitoError::Internal("Stop before the commit".to_string()))
        })
        .await
        .unwrap_err();
    assert_eq!(
        failed,
        TitoError::Internal("Stop before the commit".to_string())
    );
    assert_eq!(
        stored_value(engine, &shops.key(&stored.id)).await,
        Some(as_json(stored))
    );
    assert_eq!(get_raw(engine, &shops.key(&added.id)).await, None);
    assert_eq!(
        keys_with_prefix(engine, &scope_entries(scope)).await,
        vec![scope_entry(scope, &stored.id)]
    );
    assert_eq!(
        stored_value(engine, &scope_entry(scope, &stored.id)).await,
        Some(as_json(stored))
    );
    assert_eq!(
        fences_left(engine, "lib-model-shops", &stored.id).await,
        fences
    );
}

#[tokio::test]
async fn a_commit_that_meets_a_newer_write_is_retried_and_the_retry_reads_that_write() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let stored = &shop(&fresh_scope(), "Stored");
    create_one(shops, stored).await.unwrap();
    let attempts = &AtomicUsize::new(0);

    engine
        .transaction(|tx| async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let old = tx.batch_get(shops.read(&stored.id, Guard::None)).await?;
            if attempt == 1 {
                update_one(
                    shops,
                    &Shop {
                        name: format!("{} by another", old.name),
                        ..old.clone()
                    },
                )
                .await?;
            }
            let new = Shop {
                name: format!("{} then attempt {attempt}", old.name),
                ..old.clone()
            };
            shops.update(&old, &new, &tx).await
        })
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        read_one(shops, &stored.id)
            .await
            .unwrap()
            .map(|shop| shop.name),
        Some("Stored by another then attempt 2".to_string())
    );
}

#[tokio::test]
async fn scan_records_pages_every_record_of_its_model_in_key_order() {
    let engine = &native_engine().await;
    let ledgers = &engine.clone().model::<Ledger>();
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let ids = &["a1", "a2", "a20", "a3"].map(|suffix| format!("{scope}-{suffix}"));
    engine
        .transaction(|tx| async move {
            for id in ids.iter().rev() {
                ledgers
                    .create(
                        &Ledger {
                            id: id.clone(),
                            scope: scope.clone(),
                        },
                        &tx,
                    )
                    .await?;
            }
            Ok::<_, TitoError>(())
        })
        .await
        .unwrap();
    let one = &shop(scope, "One");
    let two = &shop(scope, "Two");
    engine
        .transaction(|tx| async move {
            shops.create(one, &tx).await?;
            shops.create(two, &tx).await
        })
        .await
        .unwrap();
    let foreign = &shops
        .query_by_index("lib-model-shop-by-scope")
        .value(text(scope))
        .limit(Some(1))
        .execute_ids(None)
        .await
        .unwrap()
        .cursor
        .expect("A page of one of two shops has a cursor");

    let (mine, sizes, refusals) = engine
        .transaction(|tx| async move {
            let mut seen = Vec::new();
            let mut sizes = Vec::new();
            let mut cursor = None;
            loop {
                let page = ledgers.scan_records(2, cursor, &tx).await?;
                sizes.push((page.items.len(), page.cursor.is_some()));
                seen.extend(page.items);
                match page.cursor {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            let refusals = vec![
                ledgers
                    .scan_records(0, None, &tx)
                    .await
                    .map(|page| page.items.len()),
                ledgers
                    .scan_records(2, Some(foreign.clone()), &tx)
                    .await
                    .map(|page| page.items.len()),
                ledgers
                    .scan_records(2, Some("not-base64".to_string()), &tx)
                    .await
                    .map(|page| page.items.len()),
            ];
            let mine = seen
                .into_iter()
                .filter(|ledger| ledger.scope == *scope)
                .collect::<Vec<_>>();
            for ledger in &mine {
                ledgers.remove(ledger, &tx).await?;
            }
            Ok::<_, TitoError>((mine, sizes, refusals))
        })
        .await
        .unwrap();
    assert!(
        sizes
            .iter()
            .all(|(count, more)| if *more { *count == 2 } else { *count <= 2 }),
        "{sizes:?}"
    );
    assert_eq!(
        mine.iter()
            .map(|ledger| ledger.id.clone())
            .collect::<Vec<_>>(),
        ids.to_vec()
    );
    assert_eq!(
        refusals,
        vec![
            Err(TitoError::InvalidInput(
                "Scan limit must be greater than zero".to_string()
            )),
            Err(TitoError::InvalidInput(
                "Cursor is outside the requested scan range".to_string()
            )),
            Err(TitoError::DeserializationFailed(
                "Failed to decode cursor".to_string()
            )),
        ]
    );
    for id in ids {
        assert_eq!(get_raw(engine, &ledgers.key(id)).await, None);
    }
}

#[tokio::test]
async fn a_save_over_the_size_bound_is_refused_before_it_writes() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let small = &shop(scope, "Small");
    create_one(shops, small).await.unwrap();
    let name = "x".repeat(MAX_SAVE_BYTES / 3 + 4096);
    let huge = &Shop {
        id: Uuid::new_v4().to_string(),
        scope: scope.clone(),
        name: name.clone(),
    };
    let grown = &Shop {
        name,
        ..small.clone()
    };

    let (created, updated) = engine
        .transaction(|tx| async move {
            let created = shops.create(huge, &tx).await;
            let stored = tx.batch_get(shops.read(&small.id, Guard::None)).await?;
            Ok::<_, TitoError>((created, shops.update(&stored, grown, &tx).await))
        })
        .await
        .unwrap();
    let refusal =
        TitoError::InvalidInput(format!("A save exceeds the {MAX_SAVE_BYTES}-byte limit"));
    assert_eq!(created, Err(refusal.clone()));
    assert_eq!(updated, Err(refusal));
    assert_eq!(get_raw(engine, &shops.key(&huge.id)).await, None);
    assert_eq!(
        stored_value(engine, &shops.key(&small.id)).await,
        Some(as_json(small))
    );
    assert_eq!(
        keys_with_prefix(engine, &scope_entries(scope)).await,
        vec![scope_entry(scope, &small.id)]
    );
}

#[tokio::test]
async fn prefix_end_closes_a_prefix_range_and_key_after_continues_after_an_exact_key() {
    assert_eq!(prefix_end("abc".to_string()), "abd");
    assert_eq!(prefix_end("ab\u{10ffff}".to_string()), "ac");
    assert_eq!(key_after("abc".to_string()), "abc\0");

    let engine = &native_engine().await;
    let prefix = &format!("lib-model-bounds:{}:", fresh_scope());
    let end = &prefix_end(prefix.clone());
    let inside = ["a2", "a20", "a3", "\u{10ffff}"].map(|suffix| format!("{prefix}{suffix}"));
    let outside = format!("{end}x");
    put_keys(engine, &[inside.to_vec(), vec![outside]].concat()).await;

    let (all, after, past) = engine
        .transaction(|tx| async move {
            let exact = format!("{prefix}a2");
            let all = tx.scan_keys(prefix.as_bytes()..end.as_bytes(), 100).await?;
            let after = tx
                .scan_keys(key_after(exact.clone()).as_bytes()..end.as_bytes(), 100)
                .await?;
            let past = tx
                .scan_keys(prefix_end(exact).as_bytes()..end.as_bytes(), 100)
                .await?;
            Ok::<_, TitoError>((all, after, past))
        })
        .await
        .unwrap();
    let strings = |keys: Vec<Vec<u8>>| {
        keys.into_iter()
            .map(|key| String::from_utf8(key).expect("Test keys are UTF-8"))
            .collect::<Vec<_>>()
    };
    assert_eq!(strings(all), inside.to_vec());
    assert_eq!(strings(after), inside[1..].to_vec());
    assert_eq!(strings(past), inside[2..].to_vec());
}

#[tokio::test]
async fn delete_range_removes_the_keys_inside_its_range_and_refuses_bad_bounds() {
    let engine = &native_engine().await;
    let base = &format!("lib-model-range:{}", fresh_scope());
    let start = format!("{base}:");
    let end = prefix_end(start.clone());
    let kept = vec![base.clone(), end.clone()];
    put_keys(
        engine,
        &[vec![format!("{start}a"), format!("{start}b")], kept.clone()].concat(),
    )
    .await;

    for (from, to, message) in [
        (
            &b""[..],
            end.as_bytes(),
            "Delete range bounds must not be empty",
        ),
        (
            start.as_bytes(),
            &b""[..],
            "Delete range bounds must not be empty",
        ),
        (
            end.as_bytes(),
            start.as_bytes(),
            "Delete range start must be less than end",
        ),
    ] {
        assert_eq!(
            engine.delete_range(from, to).await.unwrap_err(),
            TitoError::InvalidInput(message.to_string())
        );
    }
    assert_eq!(keys_with_prefix(engine, base).await.len(), 4);

    engine
        .delete_range(start.as_bytes(), end.as_bytes())
        .await
        .unwrap();
    assert_eq!(keys_with_prefix(engine, base).await, kept);
}

#[tokio::test]
async fn garbage_collection_refuses_a_zero_window_and_keeps_current_values() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let stored = &shop(&fresh_scope(), "Kept");
    create_one(shops, stored).await.unwrap();

    assert_eq!(
        engine.garbage_collect(Duration::ZERO).await.unwrap_err(),
        TitoError::InvalidInput(
            "Garbage collection retention must be greater than zero".to_string()
        )
    );
    engine
        .garbage_collect(Duration::from_secs(24 * 60 * 60))
        .await
        .unwrap();
    assert_eq!(
        read_one(shops, &stored.id).await.unwrap(),
        Some(stored.clone())
    );
}

#[tokio::test]
async fn both_model_constructions_name_the_same_keys_and_model_tx_runs_a_transaction() {
    let engine = &native_engine().await;
    let built = &TitoModel::<_, Shop>::new(engine.clone());
    let called = &engine.clone().model::<Shop>();
    let stored = &shop(&fresh_scope(), "Built");
    assert_eq!(built.get_table(), called.get_table());
    assert_eq!(built.key(&stored.id), called.key(&stored.id));

    built
        .tx(|tx| async move { built.create(stored, &tx).await })
        .await
        .unwrap();
    assert_eq!(
        read_one(called, &stored.id).await.unwrap(),
        Some(stored.clone())
    );
}

#[tokio::test]
async fn a_transaction_reads_its_own_writes_through_notes_and_index_pages() {
    let engine = &native_engine().await;
    let shops = &engine.clone().model::<Shop>();
    let scope = &fresh_scope();
    let kept = &shop(scope, "Kept");
    let gone = &shop(scope, "Gone");
    create_one(shops, gone).await.unwrap();

    let (found, listed) = engine
        .transaction(|tx| async move {
            shops.create(kept, &tx).await?;
            let stored = tx.batch_get(shops.read(&gone.id, Guard::None)).await?;
            shops.remove(&stored, &tx).await?;
            let found = tx
                .batch_get((
                    shops.read(&kept.id, Guard::None),
                    shops.read_maybe(&gone.id, Guard::None),
                    shops.read_many(vec![gone.id.clone(), kept.id.clone()], Guard::None),
                ))
                .await?;
            let listed = shops
                .query_by_index("lib-model-shop-by-scope")
                .value(text(scope))
                .execute(Some(&tx))
                .await?;
            Ok::<_, TitoError>((found, listed.items))
        })
        .await
        .unwrap();
    assert_eq!(found, (kept.clone(), None, vec![kept.clone()]));
    assert_eq!(listed, vec![kept.clone()]);
    assert_eq!(read_one(shops, &kept.id).await.unwrap(), Some(kept.clone()));
    assert_eq!(read_one(shops, &gone.id).await.unwrap(), None);
}

#[tokio::test]
async fn an_update_moves_only_the_unique_value_it_changes_and_a_remove_frees_them_all() {
    let engine = &native_engine().await;
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let first = &member(scope, "ada@example.com", "ada");
    create_one(members, first).await.unwrap();
    let renamed = &Member {
        handle: "lovelace".to_string(),
        ..first.clone()
    };
    update_one(members, renamed).await.unwrap();

    let found = engine
        .transaction(|tx| async move {
            tx.batch_get((
                members.read_unique(
                    "lib-model-member-by-email",
                    vec![text(scope), text("ada@example.com")],
                    Guard::None,
                ),
                members.read_unique(
                    "lib-model-member-by-handle",
                    vec![text(scope), text("lovelace")],
                    Guard::None,
                ),
                members.read_unique_maybe(
                    "lib-model-member-by-handle",
                    vec![text(scope), text("ada")],
                    Guard::None,
                ),
            ))
            .await
        })
        .await
        .unwrap();
    assert_eq!(found, (renamed.clone(), renamed.clone(), None));

    remove_one(members, &first.id).await.unwrap();
    assert_eq!(get_raw(engine, &members.key(&first.id)).await, None);
    assert!(
        keys_with_prefix(engine, &format!("index:lib-model-member-by-email:{scope}:"))
            .await
            .is_empty()
    );
    assert!(keys_with_prefix(
        engine,
        &format!("index:lib-model-member-by-handle:{scope}:")
    )
    .await
    .is_empty());
}

#[tokio::test]
async fn unique_values_freed_earlier_in_a_transaction_can_be_taken_later_in_it() {
    let engine = &native_engine().await;
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let first = &member(scope, "ada@example.com", "ada");
    create_one(members, first).await.unwrap();
    let renamed = &Member {
        handle: "lovelace".to_string(),
        ..first.clone()
    };
    let second = &member(scope, "grace@example.com", "ada");
    let third = &member(scope, "ada@example.com", "lovelace");

    engine
        .transaction(|tx| async move {
            let stored = tx.batch_get(members.read(&first.id, Guard::None)).await?;
            members.update(&stored, renamed, &tx).await?;
            members.create(second, &tx).await?;
            members.remove(renamed, &tx).await?;
            members.create(third, &tx).await
        })
        .await
        .unwrap();

    let found = engine
        .transaction(|tx| async move {
            tx.batch_get((
                members.read_unique(
                    "lib-model-member-by-handle",
                    vec![text(scope), text("ada")],
                    Guard::None,
                ),
                members.read_unique(
                    "lib-model-member-by-email",
                    vec![text(scope), text("ada@example.com")],
                    Guard::None,
                ),
                members.read_unique(
                    "lib-model-member-by-handle",
                    vec![text(scope), text("lovelace")],
                    Guard::None,
                ),
                members.read_maybe(&first.id, Guard::None),
            ))
            .await
        })
        .await
        .unwrap();
    assert_eq!(found, (second.clone(), third.clone(), third.clone(), None));
}

#[tokio::test]
async fn a_save_refused_inside_its_transaction_ends_it_and_commits_none_of_its_writes() {
    let engine = &native_engine().await;
    let members = &engine.clone().model::<Member>();
    let scope = &fresh_scope();
    let first = &member(scope, "ada@example.com", "ada");
    let second = &member(scope, "grace@example.com", "ada");
    let taken = TitoError::UniqueViolation {
        model: "lib-model-members".to_string(),
        index: "lib-model-member-by-handle".to_string(),
        values: vec![text(scope), text("ada")],
    };
    let email_entry = |email: &str| format!("index:lib-model-member-by-email:{scope}:{email}:");

    let created = engine
        .transaction(|tx| async move {
            members.create(first, &tx).await?;
            Ok::<_, TitoError>(members.create(second, &tx).await)
        })
        .await;
    assert_eq!(created, Err(taken.clone()));
    assert_eq!(get_raw(engine, &members.key(&first.id)).await, None);
    assert_eq!(get_raw(engine, &members.key(&second.id)).await, None);
    assert_eq!(
        get_raw(engine, &email_entry("grace@example.com")).await,
        None
    );

    let other = &member(scope, "grace@example.com", "grace");
    create_one(members, other).await.unwrap();
    let stored = get_raw(engine, &members.key(&other.id)).await;
    assert!(stored.is_some());
    let moved = &Member {
        email: "hopper@example.com".to_string(),
        handle: "ada".to_string(),
        ..other.clone()
    };
    let updated = engine
        .transaction(|tx| async move {
            members.create(first, &tx).await?;
            let old = tx.batch_get(members.read(&other.id, Guard::None)).await?;
            Ok::<_, TitoError>(members.update(&old, moved, &tx).await)
        })
        .await;
    assert_eq!(updated, Err(taken));
    assert_eq!(get_raw(engine, &members.key(&first.id)).await, None);
    assert_eq!(get_raw(engine, &members.key(&other.id)).await, stored);
    assert_eq!(
        get_raw(engine, &email_entry("grace@example.com")).await,
        stored
    );
    assert_eq!(
        get_raw(engine, &email_entry("hopper@example.com")).await,
        None
    );
}
