use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::backend::tikv::TiKVBackend;
use crate::query::IndexQueryBuilder;
use crate::queue::test_engine::{get_raw, keys_with_prefix, native_engine, put_raw};
use crate::types::{TitoEngine, TitoModelConstraints, TitoTransaction};
use crate::{
    encode_index_integer, encode_index_string, Batch, Guard, TitoError, TitoIndex, TitoIndexBound,
    TitoIndexField, TitoIndexValue, TitoModel, TitoModelTrait,
};

const ITEMS: &str = "lib-index-items";
const ITEM_BY_RANK: &str = "lib-index-item-by-rank";
const ITEM_BY_CODE: &str = "lib-index-item-by-code";
const ITEM_BY_TAG: &str = "lib-index-item-by-tag";
const ITEM_SUMMARY: &str = "lib-index-item-summary";
const ITEM_SUMMARY_PATHS: &[&str] = &["id", "rank", "contact.email", "lines.sku"];

const REFUSED: &str = "lib-index-refused";
const REFUSED_BY_SCOPE: &str = "lib-index-refused-by-scope";
const REFUSED_PATHS: &[&str] = &["id"];
const REFUSED_UNIQUE_BY_SCOPE: &str = "lib-index-refused-unique-by-scope";

const REBUILT: &str = "lib-index-rebuilt";
const REBUILT_BY_GROUP: &str = "lib-index-rebuilt-by-group";
const REBUILT_BY_CODE: &str = "lib-index-rebuilt-by-code";

const CLAIMED: &str = "lib-index-claimed";
const CLAIMED_BY_CODE: &str = "lib-index-claimed-by-code";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Item {
    id: String,
    scope: String,
    code: String,
    rank: i64,
    tags: Vec<String>,
    contact: Contact,
    lines: Vec<Line>,
    body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Contact {
    email: String,
    phone: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Line {
    sku: String,
    quantity: i64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ItemSummary {
    id: String,
    rank: i64,
    contact: SummaryContact,
    lines: Vec<SummaryLine>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SummaryContact {
    email: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SummaryLine {
    sku: String,
}

impl TitoModelTrait for Item {
    fn indexes(&self) -> Vec<TitoIndex> {
        let mut indexes = vec![
            TitoIndex {
                name: ITEM_BY_RANK,
                fields: vec![text(&self.scope), number(self.rank)],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: ITEM_BY_CODE,
                fields: vec![text(&self.scope), text(&self.code)],
                value: TitoIndexValue::Full,
                unique: true,
            },
            TitoIndex {
                name: ITEM_SUMMARY,
                fields: vec![text(&self.scope), number(self.rank)],
                value: TitoIndexValue::Part(ITEM_SUMMARY_PATHS),
                unique: false,
            },
        ];
        for tag in &self.tags {
            indexes.push(TitoIndex {
                name: ITEM_BY_TAG,
                fields: vec![text(&self.scope), text(tag)],
                value: TitoIndexValue::KeysOnly,
                unique: false,
            });
        }
        indexes
    }

    fn table() -> String {
        ITEMS.to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum Rule {
    Kept,
    UniqueTwice,
    NoField,
    EmptyText,
    UniqueKeysOnly,
    UniquePart,
    EmptyPart,
    EmptyName,
    NameWithColon,
    NameWithBackslash,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Refused {
    id: String,
    scope: String,
    rule: Rule,
}

impl TitoModelTrait for Refused {
    fn indexes(&self) -> Vec<TitoIndex> {
        let scope = text(&self.scope);
        let mut indexes = vec![TitoIndex {
            name: REFUSED_BY_SCOPE,
            fields: vec![scope.clone()],
            value: TitoIndexValue::Full,
            unique: false,
        }];
        let broken = match self.rule {
            Rule::Kept => return indexes,
            Rule::UniqueTwice => {
                for _ in 0..2 {
                    indexes.push(TitoIndex {
                        name: REFUSED_UNIQUE_BY_SCOPE,
                        fields: vec![scope.clone()],
                        value: TitoIndexValue::Full,
                        unique: true,
                    });
                }
                return indexes;
            }
            Rule::NoField => TitoIndex {
                name: "lib-index-refused-no-field",
                fields: Vec::new(),
                value: TitoIndexValue::Full,
                unique: false,
            },
            Rule::EmptyText => TitoIndex {
                name: "lib-index-refused-empty-text",
                fields: vec![scope, text("")],
                value: TitoIndexValue::Full,
                unique: false,
            },
            Rule::UniqueKeysOnly => TitoIndex {
                name: "lib-index-refused-unique-keys-only",
                fields: vec![scope],
                value: TitoIndexValue::KeysOnly,
                unique: true,
            },
            Rule::UniquePart => TitoIndex {
                name: "lib-index-refused-unique-part",
                fields: vec![scope],
                value: TitoIndexValue::Part(REFUSED_PATHS),
                unique: true,
            },
            Rule::EmptyPart => TitoIndex {
                name: "lib-index-refused-empty-part",
                fields: vec![scope],
                value: TitoIndexValue::Part(&[]),
                unique: false,
            },
            Rule::EmptyName => TitoIndex {
                name: "",
                fields: vec![scope],
                value: TitoIndexValue::Full,
                unique: false,
            },
            Rule::NameWithColon => TitoIndex {
                name: "lib-index:refused",
                fields: vec![scope],
                value: TitoIndexValue::Full,
                unique: false,
            },
            Rule::NameWithBackslash => TitoIndex {
                name: "lib-index\\refused",
                fields: vec![scope],
                value: TitoIndexValue::Full,
                unique: false,
            },
        };
        indexes.push(broken);
        indexes
    }

    fn table() -> String {
        REFUSED.to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Rebuilt {
    id: String,
    group: String,
    code: String,
    rank: i64,
}

impl TitoModelTrait for Rebuilt {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![
            TitoIndex {
                name: REBUILT_BY_GROUP,
                fields: vec![text(&self.group), number(self.rank)],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: REBUILT_BY_CODE,
                fields: vec![text(&self.code)],
                value: TitoIndexValue::Full,
                unique: true,
            },
        ]
    }

    fn table() -> String {
        REBUILT.to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Claimed {
    id: String,
    code: String,
}

impl TitoModelTrait for Claimed {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: CLAIMED_BY_CODE,
            fields: vec![text(&self.code)],
            value: TitoIndexValue::Full,
            unique: true,
        }]
    }

    fn table() -> String {
        CLAIMED.to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn text(value: &str) -> TitoIndexField {
    TitoIndexField::Text(value.to_string())
}

fn number(value: i64) -> TitoIndexField {
    TitoIndexField::Number(value)
}

fn included(fields: Vec<TitoIndexField>) -> Option<TitoIndexBound> {
    Some(TitoIndexBound::Included(fields))
}

fn excluded(fields: Vec<TitoIndexField>) -> Option<TitoIndexBound> {
    Some(TitoIndexBound::Excluded(fields))
}

fn fresh_scope() -> String {
    Uuid::new_v4().simple().to_string()
}

fn fresh_id() -> String {
    Uuid::new_v4().to_string()
}

fn item(scope: &str, rank: i64, code: &str) -> Item {
    Item {
        id: fresh_id(),
        scope: scope.to_string(),
        code: code.to_string(),
        rank,
        tags: Vec::new(),
        contact: Contact {
            email: format!("{code}@example.com"),
            phone: "+10000000000".to_string(),
        },
        lines: vec![
            Line {
                sku: format!("{code}-first"),
                quantity: 1,
            },
            Line {
                sku: format!("{code}-second"),
                quantity: 2,
            },
        ],
        body: "body".to_string(),
    }
}

fn summary_of(record: &Item) -> ItemSummary {
    ItemSummary {
        id: record.id.clone(),
        rank: record.rank,
        contact: SummaryContact {
            email: record.contact.email.clone(),
        },
        lines: record
            .lines
            .iter()
            .map(|line| SummaryLine {
                sku: line.sku.clone(),
            })
            .collect(),
    }
}

fn record_key(record: &Item) -> String {
    format!("table:{ITEMS}:{}", record.id)
}

fn rank_key(record: &Item) -> String {
    format!(
        "index:{ITEM_BY_RANK}:{}:{}:{}:",
        encode_index_string(&record.scope),
        encode_index_integer(record.rank),
        encode_index_string(&record.id)
    )
}

fn code_key(record: &Item) -> String {
    format!(
        "index:{ITEM_BY_CODE}:{}:{}:",
        encode_index_string(&record.scope),
        encode_index_string(&record.code)
    )
}

fn summary_key(record: &Item) -> String {
    format!(
        "index:{ITEM_SUMMARY}:{}:{}:{}:",
        encode_index_string(&record.scope),
        encode_index_integer(record.rank),
        encode_index_string(&record.id)
    )
}

fn tag_key(record: &Item, tag: &str) -> String {
    format!(
        "index:{ITEM_BY_TAG}:{}:{}:{}:",
        encode_index_string(&record.scope),
        encode_index_string(tag),
        encode_index_string(&record.id)
    )
}

fn code_values(record: &Item) -> Vec<TitoIndexField> {
    vec![text(&record.scope), text(&record.code)]
}

fn violation(scope: &str, code: &str) -> TitoError {
    TitoError::UniqueViolation {
        model: ITEMS.to_string(),
        index: ITEM_BY_CODE.to_string(),
        values: vec![text(scope), text(code)],
    }
}

fn rebuilt_key(record: &Rebuilt) -> String {
    format!("table:{REBUILT}:{}", record.id)
}

fn group_key(record: &Rebuilt) -> String {
    format!(
        "index:{REBUILT_BY_GROUP}:{}:{}:{}:",
        record.group,
        encode_index_integer(record.rank),
        record.id
    )
}

fn rebuilt_code_key(record: &Rebuilt) -> String {
    format!("index:{REBUILT_BY_CODE}:{}:", record.code)
}

fn rank_query(
    items: &TitoModel<TiKVBackend, Item>,
    scope: &str,
) -> IndexQueryBuilder<TiKVBackend, Item> {
    let mut query = items.query_by_index(ITEM_BY_RANK);
    query.value(text(scope));
    query
}

fn assert_index_error<P: std::fmt::Debug>(outcome: Result<P, TitoError>, key: &str) {
    match outcome {
        Err(TitoError::IndexError(message)) => {
            assert!(message.contains(key), "'{message}' does not name '{key}'")
        }
        other => panic!("Expected an index error naming '{key}', got {other:?}"),
    }
}

async fn create<T: TitoModelConstraints>(
    model: &TitoModel<TiKVBackend, T>,
    records: &[T],
) -> Result<(), TitoError> {
    let records = records.to_vec();
    model
        .engine
        .transaction(move |tx| async move {
            for record in &records {
                model.create(record, &tx).await?;
            }
            Ok::<_, TitoError>(())
        })
        .await
}

async fn fetch<B>(engine: &TiKVBackend, batch: B) -> Result<B::Output, TitoError>
where
    B: Batch<Error = TitoError> + Clone,
{
    engine
        .transaction(move |tx| async move { tx.batch_get(batch).await })
        .await
}

async fn replace(
    items: &TitoModel<TiKVBackend, Item>,
    old: &Item,
    new: &Item,
) -> Result<(), TitoError> {
    items
        .engine
        .transaction(move |tx| async move {
            let stored = tx.batch_get(items.read(&old.id, Guard::None)).await?;
            assert_eq!(stored, *old);
            items.update(&stored, new, &tx).await
        })
        .await
}

async fn remove_record(items: &TitoModel<TiKVBackend, Item>, id: &str) -> Result<(), TitoError> {
    items
        .engine
        .transaction(move |tx| async move {
            let record = tx.batch_get(items.read(id, Guard::None)).await?;
            items.remove(&record, &tx).await
        })
        .await
}

async fn recode(
    items: &TitoModel<TiKVBackend, Item>,
    changes: &[(&str, &str)],
) -> Result<(), TitoError> {
    let changes = changes
        .iter()
        .map(|(id, code)| (id.to_string(), code.to_string()))
        .collect::<Vec<_>>();
    let mut ids = changes.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    items
        .engine
        .transaction(move |tx| async move {
            let mut current = tx.batch_get(items.read_many(ids, Guard::None)).await?;
            for (id, code) in &changes {
                let Some(position) = current.iter().position(|record| record.id == *id) else {
                    return Err(TitoError::NotFound(id.clone()));
                };
                let next = Item {
                    code: code.clone(),
                    ..current[position].clone()
                };
                items.update(&current[position], &next, &tx).await?;
                current[position] = next;
            }
            Ok::<_, TitoError>(())
        })
        .await
}

async fn delete_raw(engine: &TiKVBackend, keys: Vec<String>) {
    engine
        .transaction(move |tx| async move {
            for key in &keys {
                tx.delete(key).await?;
            }
            Ok::<_, TitoError>(())
        })
        .await
        .expect("Delete raw test rows");
}

async fn listed(
    items: &TitoModel<TiKVBackend, Item>,
    scope: &str,
    lower: Option<TitoIndexBound>,
    upper: Option<TitoIndexBound>,
    descending: bool,
) -> Result<Vec<(i64, String)>, TitoError> {
    let mut query = rank_query(items, scope);
    if let Some(lower) = lower {
        query.lower(lower);
    }
    if let Some(upper) = upper {
        query.upper(upper);
    }
    if descending {
        query.descending();
    }
    Ok(query
        .execute(None)
        .await?
        .items
        .into_iter()
        .map(|record| (record.rank, record.id))
        .collect())
}

#[tokio::test]
async fn full_keys_only_and_part_entries_hold_the_record_nothing_and_the_named_fields() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let record = Item {
        tags: vec![
            "green".to_string(),
            "dup".to_string(),
            "dup".to_string(),
            "Dup".to_string(),
        ],
        ..item(&scope, 7, "code-1")
    };
    create(&items, &[record.clone()]).await.unwrap();

    let stored = get_raw(&engine, &record_key(&record)).await.unwrap();
    assert_eq!(serde_json::from_slice::<Item>(&stored).unwrap(), record);
    assert_eq!(
        get_raw(&engine, &rank_key(&record)).await,
        Some(stored.clone())
    );
    assert_eq!(
        get_raw(&engine, &code_key(&record)).await,
        Some(stored.clone())
    );
    let part = get_raw(&engine, &summary_key(&record)).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&part).unwrap(),
        json!([
            record.id,
            {
                "id": record.id,
                "rank": 7,
                "contact": { "email": "code-1@example.com" },
                "lines": [{ "sku": "code-1-first" }, { "sku": "code-1-second" }]
            }
        ])
    );
    let mut tags = vec![
        tag_key(&record, "green"),
        tag_key(&record, "dup"),
        tag_key(&record, "Dup"),
    ];
    tags.sort();
    assert_eq!(
        keys_with_prefix(&engine, &format!("index:{ITEM_BY_TAG}:{scope}:")).await,
        tags
    );
    for key in &tags {
        assert_eq!(get_raw(&engine, key).await, Some(Vec::new()), "{key}");
    }
    for (index, key) in [
        (ITEM_BY_RANK, rank_key(&record)),
        (ITEM_BY_CODE, code_key(&record)),
        (ITEM_SUMMARY, summary_key(&record)),
    ] {
        assert_eq!(
            keys_with_prefix(&engine, &format!("index:{index}:{scope}:")).await,
            vec![key]
        );
    }
}

#[tokio::test]
async fn a_unique_index_listed_twice_writes_one_entry() {
    let engine = native_engine().await;
    let refused = engine.clone().model::<Refused>();
    let record = Refused {
        id: fresh_id(),
        scope: fresh_scope(),
        rule: Rule::UniqueTwice,
    };
    create(&refused, &[record.clone()]).await.unwrap();
    let stored = get_raw(&engine, &format!("table:{REFUSED}:{}", record.id))
        .await
        .unwrap();
    assert_eq!(
        get_raw(
            &engine,
            &format!("index:{REFUSED_UNIQUE_BY_SCOPE}:{}:", record.scope)
        )
        .await,
        Some(stored)
    );
}

#[tokio::test]
async fn text_values_are_escaped_and_found_only_by_their_exact_value() {
    assert_eq!(encode_index_string("a:b\\c"), "a\\:b\\\\c");
    assert_eq!(encode_index_string(" Ab-c "), " Ab-c ");
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let values = ["a", "a:b", "a\\b", "A:B", " a:b", "a:b ", "a b"];
    let records = values
        .iter()
        .enumerate()
        .map(|(position, value)| Item {
            tags: vec![value.to_string()],
            ..item(&scope, position as i64, &format!("code-{position}"))
        })
        .collect::<Vec<_>>();
    create(&items, &records).await.unwrap();

    for (value, record) in values.iter().zip(&records) {
        let mut query = items.query_by_index(ITEM_BY_TAG);
        query.value(text(&scope)).value(text(value));
        assert_eq!(
            query.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()],
            "{value:?}"
        );
    }
    for (prefix, record) in [
        (format!("index:{ITEM_BY_TAG}:{scope}:a:"), &records[0]),
        (format!("index:{ITEM_BY_TAG}:{scope}:a\\:b:"), &records[1]),
        (format!("index:{ITEM_BY_TAG}:{scope}:a\\\\b:"), &records[2]),
        (format!("index:{ITEM_BY_TAG}:{scope}:A\\:B:"), &records[3]),
    ] {
        assert_eq!(
            keys_with_prefix(&engine, &prefix).await,
            vec![format!("{prefix}{}:", record.id)]
        );
    }
}

#[tokio::test]
async fn numbers_keep_their_order_from_the_smallest_through_zero_to_the_largest() {
    assert_eq!(encode_index_integer(i64::MIN), "00000000000000000000");
    assert_eq!(encode_index_integer(-1), "09223372036854775807");
    assert_eq!(encode_index_integer(0), "09223372036854775808");
    assert_eq!(encode_index_integer(1), "09223372036854775809");
    assert_eq!(encode_index_integer(i64::MAX), "18446744073709551615");
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let ranks = [
        i64::MAX,
        1,
        -1,
        10_000_000_000,
        0,
        i64::MIN,
        -10_000_000_000,
        9_999_999_999,
        -9,
    ];
    let records = ranks
        .iter()
        .enumerate()
        .map(|(position, rank)| item(&scope, *rank, &format!("code-{position}")))
        .collect::<Vec<_>>();
    create(&items, &records).await.unwrap();

    let mut sorted = ranks.to_vec();
    sorted.sort_unstable();
    let mut query = rank_query(&items, &scope);
    let ascending = query.execute(None).await.unwrap();
    assert_eq!(
        ascending
            .items
            .iter()
            .map(|record| record.rank)
            .collect::<Vec<_>>(),
        sorted
    );
    assert!(ascending.cursor.is_none());
    query.descending();
    sorted.reverse();
    assert_eq!(
        query
            .execute(None)
            .await
            .unwrap()
            .items
            .iter()
            .map(|record| record.rank)
            .collect::<Vec<_>>(),
        sorted
    );
    let prefix = format!("index:{ITEM_BY_RANK}:{scope}:09223372036854775807:");
    assert_eq!(
        keys_with_prefix(&engine, &prefix).await,
        vec![format!("{prefix}{}:", records[2].id)]
    );
    for rank in ranks {
        let mut exact = rank_query(&items, &scope);
        exact.value(number(rank));
        assert_eq!(
            exact
                .execute(None)
                .await
                .unwrap()
                .items
                .iter()
                .map(|record| record.rank)
                .collect::<Vec<_>>(),
            vec![rank]
        );
    }
}

#[tokio::test]
async fn bounds_pick_a_range_of_the_fields_after_the_values() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let records = [-2, -1, 0, 0, 0, 1, 2]
        .iter()
        .enumerate()
        .map(|(position, rank)| item(&scope, *rank, &format!("code-{position}")))
        .collect::<Vec<_>>();
    create(&items, &records).await.unwrap();
    let mut entries = records
        .iter()
        .map(|record| (record.rank, record.id.clone()))
        .collect::<Vec<_>>();
    entries.sort();
    let between = |low: i64, high: i64| {
        entries
            .iter()
            .filter(|(rank, _)| (low..=high).contains(rank))
            .cloned()
            .collect::<Vec<_>>()
    };

    assert_eq!(
        listed(
            &items,
            &scope,
            included(vec![number(-1)]),
            excluded(vec![number(2)]),
            false
        )
        .await
        .unwrap(),
        between(-1, 1)
    );
    assert_eq!(
        listed(
            &items,
            &scope,
            excluded(vec![number(-1)]),
            included(vec![number(1)]),
            false
        )
        .await
        .unwrap(),
        between(0, 1)
    );
    assert_eq!(
        listed(&items, &scope, None, excluded(vec![number(0)]), false)
            .await
            .unwrap(),
        between(-2, -1)
    );
    assert_eq!(
        listed(&items, &scope, included(vec![number(2)]), None, false)
            .await
            .unwrap(),
        between(2, 2)
    );
    let mut reversed = between(-1, 1);
    reversed.reverse();
    assert_eq!(
        listed(
            &items,
            &scope,
            included(vec![number(-1)]),
            included(vec![number(1)]),
            true
        )
        .await
        .unwrap(),
        reversed
    );

    let middle = entries[3].clone();
    let at_middle = vec![number(middle.0), text(&middle.1)];
    let mut before = entries[..3].to_vec();
    before.reverse();
    assert_eq!(
        listed(&items, &scope, None, excluded(at_middle.clone()), true)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        listed(&items, &scope, excluded(at_middle.clone()), None, false)
            .await
            .unwrap(),
        entries[4..].to_vec()
    );
    assert_eq!(
        listed(
            &items,
            &scope,
            included(at_middle.clone()),
            included(at_middle),
            false
        )
        .await
        .unwrap(),
        vec![middle]
    );
    assert!(matches!(
        listed(&items, &scope, included(Vec::new()), None, false).await,
        Err(TitoError::IndexError(_))
    ));
}

#[tokio::test]
async fn a_bound_ending_with_a_record_id_splits_the_scan_at_that_record() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let records = ["a2", "a20", "a3"]
        .iter()
        .map(|suffix| {
            let id = format!("{scope}-{suffix}");
            Item {
                id: id.clone(),
                ..item(&scope, 0, &id)
            }
        })
        .collect::<Vec<_>>();
    create(&items, &records).await.unwrap();
    let all = listed(&items, &scope, None, None, false).await.unwrap();
    assert_eq!(all.len(), records.len());

    for (position, (rank, id)) in all.iter().enumerate() {
        let at = vec![number(*rank), text(id)];
        assert_eq!(
            listed(&items, &scope, included(at.clone()), None, false)
                .await
                .unwrap(),
            all[position..].to_vec(),
            "from {id}"
        );
        assert_eq!(
            listed(&items, &scope, excluded(at.clone()), None, false)
                .await
                .unwrap(),
            all[position + 1..].to_vec(),
            "after {id}"
        );
        assert_eq!(
            listed(&items, &scope, None, included(at.clone()), false)
                .await
                .unwrap(),
            all[..=position].to_vec(),
            "through {id}"
        );
        assert_eq!(
            listed(&items, &scope, None, excluded(at), false)
                .await
                .unwrap(),
            all[..position].to_vec(),
            "before {id}"
        );
    }
}

#[tokio::test]
async fn pages_follow_their_cursors_both_ways_without_skipping_an_id_that_extends_another() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let mut records = Vec::new();
    for rank in [-1, 0, 1] {
        for suffix in ["a20", "a2", "a3"] {
            let id = format!("{scope}-{}-{suffix}", rank + 1);
            records.push(Item {
                id: id.clone(),
                ..item(&scope, rank, &id)
            });
        }
    }
    create(&items, &records).await.unwrap();
    let ids = records
        .iter()
        .map(|record| record.id.clone())
        .collect::<Vec<_>>();

    for descending in [false, true] {
        let mut expected = ids.clone();
        if descending {
            expected.reverse();
        }
        let mut cursor: Option<String> = None;
        let mut seen = Vec::new();
        let mut pages = 0;
        loop {
            let mut query = rank_query(&items, &scope);
            query.limit(Some(2)).cursor(cursor.clone());
            if descending {
                query.descending();
            }
            let page = query.execute(None).await.unwrap();
            pages += 1;
            assert!(!page.items.is_empty() && page.items.len() <= 2);
            seen.extend(page.items.into_iter().map(|record| record.id));
            cursor = page.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, expected);
        assert_eq!(pages, 5);
    }

    let mut everything = rank_query(&items, &scope);
    let all = everything.execute(None).await.unwrap();
    assert_eq!(
        all.items
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    assert!(all.cursor.is_none());
    everything.limit(Some(9));
    assert!(everything.execute(None).await.unwrap().cursor.is_none());
    everything.limit(Some(8));
    let short = everything.execute(None).await.unwrap();
    assert_eq!(short.items.len(), 8);
    let cursor = short.cursor.unwrap();
    everything.limit(Some(0));
    assert_eq!(
        everything.execute(None).await.unwrap_err(),
        TitoError::InvalidInput("Index query limit must be greater than zero".to_string())
    );

    let mut elsewhere = rank_query(&items, &fresh_scope());
    elsewhere.cursor(Some(cursor.clone()));
    assert_eq!(
        elsewhere.execute(None).await.unwrap_err(),
        TitoError::InvalidInput("Cursor is outside the requested scan range".to_string())
    );
    let mut summaries = items.query_by_index(ITEM_SUMMARY);
    summaries.value(text(&scope)).cursor(Some(cursor));
    assert_eq!(
        summaries
            .execute_part::<ItemSummary>(None)
            .await
            .unwrap_err(),
        TitoError::InvalidInput("Cursor is outside the requested scan range".to_string())
    );
    let mut garbage = rank_query(&items, &scope);
    garbage.cursor(Some("not-a-cursor"));
    assert!(matches!(
        garbage.execute(None).await,
        Err(TitoError::DeserializationFailed(_))
    ));
}

#[tokio::test]
async fn execute_reads_full_entries_execute_part_reads_part_entries_and_execute_ids_reads_all_three(
) {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let record = Item {
        tags: vec!["blue".to_string()],
        ..item(&scope, 3, "code-1")
    };
    create(&items, &[record.clone()]).await.unwrap();

    let mut full = rank_query(&items, &scope);
    assert_eq!(
        full.execute(None).await.unwrap().items,
        vec![record.clone()]
    );
    assert_eq!(
        full.execute_ids(None).await.unwrap().items,
        vec![record.id.clone()]
    );
    assert_index_error(
        full.execute_part::<ItemSummary>(None).await,
        &rank_key(&record),
    );

    let mut part = items.query_by_index(ITEM_SUMMARY);
    part.value(text(&scope));
    assert_eq!(
        part.execute_part::<ItemSummary>(None).await.unwrap().items,
        vec![summary_of(&record)]
    );
    assert_eq!(
        part.execute_ids(None).await.unwrap().items,
        vec![record.id.clone()]
    );
    assert_index_error(part.execute(None).await, &summary_key(&record));

    let mut keys_only = items.query_by_index(ITEM_BY_TAG);
    keys_only.value(text(&scope));
    assert_eq!(
        keys_only.execute_ids(None).await.unwrap().items,
        vec![record.id.clone()]
    );
    assert_index_error(keys_only.execute(None).await, &tag_key(&record, "blue"));
    assert_index_error(
        keys_only.execute_part::<ItemSummary>(None).await,
        &tag_key(&record, "blue"),
    );

    let mut unique = items.query_by_index(ITEM_BY_CODE);
    unique.value(text(&scope));
    assert_eq!(
        unique.execute(None).await.unwrap().items,
        vec![record.clone()]
    );
    unique.value(text(&record.code));
    assert_eq!(
        unique.execute(None).await.unwrap().items,
        vec![record.clone()]
    );

    delete_raw(&engine, vec![record_key(&record)]).await;
    assert_eq!(
        full.execute(None).await.unwrap().items,
        vec![record.clone()]
    );
    assert_eq!(
        fetch(
            &engine,
            items.read_unique(ITEM_BY_CODE, code_values(&record), Guard::None)
        )
        .await,
        Ok(record.clone())
    );
    assert!(matches!(
        fetch(&engine, items.read(&record.id, Guard::None)).await,
        Err(TitoError::NotFound(_))
    ));
}

#[tokio::test]
async fn an_entry_whose_bytes_disagree_with_its_key_fails_the_read_naming_the_key() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let record = item(&scope, 5, "code-1");
    let neighbour = item(&scope, 6, "code-2");
    create(&items, &[record.clone(), neighbour.clone()])
        .await
        .unwrap();
    let mut full = rank_query(&items, &scope);
    let key = rank_key(&record);

    let moved = Item {
        rank: 9,
        ..record.clone()
    };
    put_raw(&engine, &key, serde_json::to_vec(&moved).unwrap()).await;
    assert_index_error(full.execute(None).await, &key);
    assert_index_error(full.execute_ids(None).await, &key);

    put_raw(&engine, &key, br#"{"id":"other"}"#.to_vec()).await;
    match full.execute(None).await {
        Err(TitoError::DeserializationFailed(message)) => {
            assert!(message.contains(&key), "'{message}' does not name '{key}'")
        }
        other => panic!("Expected a deserialization error naming '{key}', got {other:?}"),
    }

    put_raw(&engine, &key, b"x".to_vec()).await;
    assert_index_error(full.execute(None).await, &key);

    let part_key = summary_key(&record);
    let foreign = json!([
        neighbour.id,
        {
            "id": neighbour.id,
            "rank": 5,
            "contact": { "email": "code-2@example.com" },
            "lines": []
        }
    ]);
    put_raw(&engine, &part_key, serde_json::to_vec(&foreign).unwrap()).await;
    let mut part = items.query_by_index(ITEM_SUMMARY);
    part.value(text(&scope));
    assert_index_error(part.execute_part::<ItemSummary>(None).await, &part_key);
    assert_index_error(part.execute_ids(None).await, &part_key);

    let unique_key = code_key(&record);
    let recoded = Item {
        code: "code-9".to_string(),
        ..record.clone()
    };
    put_raw(&engine, &unique_key, serde_json::to_vec(&recoded).unwrap()).await;
    assert_index_error(
        fetch(
            &engine,
            items.read_unique(ITEM_BY_CODE, code_values(&record), Guard::None),
        )
        .await,
        &unique_key,
    );
}

#[tokio::test]
async fn read_unique_finds_the_holder_and_read_unique_maybe_finds_none() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let records = ["Code", "code", " code", "code ", "co:de"]
        .iter()
        .enumerate()
        .map(|(position, code)| item(&scope, position as i64, code))
        .collect::<Vec<_>>();
    create(&items, &records).await.unwrap();

    for record in &records {
        assert_eq!(
            fetch(
                &engine,
                items.read_unique(ITEM_BY_CODE, code_values(record), Guard::None)
            )
            .await,
            Ok(record.clone())
        );
        assert_eq!(
            fetch(
                &engine,
                items.read_unique_maybe(ITEM_BY_CODE, code_values(record), Guard::None)
            )
            .await,
            Ok(Some(record.clone()))
        );
    }
    let nobody = vec![text(&scope), text("CODE")];
    assert_eq!(
        fetch(
            &engine,
            items.read_unique(ITEM_BY_CODE, nobody.clone(), Guard::None)
        )
        .await,
        Err(TitoError::NotFound(format!(
            "No record found for unique index '{ITEM_BY_CODE}'"
        )))
    );
    assert_eq!(
        fetch(
            &engine,
            items.read_unique_maybe(ITEM_BY_CODE, nobody, Guard::None)
        )
        .await,
        Ok(None)
    );
    for (index, values) in [
        (ITEM_BY_CODE, Vec::new()),
        (ITEM_BY_CODE, vec![text(&scope), text("")]),
        ("lib-index:item", vec![text(&scope)]),
        ("", vec![text(&scope)]),
    ] {
        assert!(matches!(
            fetch(&engine, items.read_unique_maybe(index, values, Guard::None)).await,
            Err(TitoError::IndexError(_))
        ));
    }
}

#[tokio::test]
async fn a_unique_value_a_committed_record_holds_refuses_a_create_and_an_update_at_commit() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let holder = item(&scope, 1, "code-1");
    create(&items, &[holder.clone()]).await.unwrap();

    let copy = item(&scope, 2, "code-1");
    let refused = create(&items, &[copy.clone()]).await.unwrap_err();
    assert_eq!(refused, violation(&scope, "code-1"));
    assert_eq!(
        refused.to_string(),
        format!("Unique index '{ITEM_BY_CODE}' on model '{ITEMS}' is already owned")
    );
    assert_eq!(get_raw(&engine, &record_key(&copy)).await, None);
    assert_eq!(get_raw(&engine, &rank_key(&copy)).await, None);

    let other = item(&scope, 3, "code-2");
    create(&items, &[other.clone()]).await.unwrap();
    assert_eq!(
        recode(&items, &[(other.id.as_str(), "code-1")]).await,
        Err(violation(&scope, "code-1"))
    );
    assert_eq!(
        fetch(
            &engine,
            items.read_unique(ITEM_BY_CODE, code_values(&other), Guard::None)
        )
        .await,
        Ok(other.clone())
    );
    assert_eq!(
        fetch(
            &engine,
            items.read_unique(ITEM_BY_CODE, code_values(&holder), Guard::None)
        )
        .await,
        Ok(holder.clone())
    );

    let twins = [item(&scope, 4, "code-3"), item(&scope, 5, "code-3")];
    assert_eq!(
        create(&items, &twins).await,
        Err(violation(&scope, "code-3"))
    );
    for twin in &twins {
        assert_eq!(get_raw(&engine, &record_key(twin)).await, None);
    }
}

#[tokio::test]
async fn a_unique_value_passes_to_another_record_when_its_holder_gives_it_up_first() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let first = item(&scope, 1, "code-1");
    let second = item(&scope, 2, "code-2");
    create(&items, &[first.clone(), second.clone()])
        .await
        .unwrap();

    recode(
        &items,
        &[
            (first.id.as_str(), "code-3"),
            (second.id.as_str(), "code-1"),
        ],
    )
    .await
    .unwrap();

    let first_now = Item {
        code: "code-3".to_string(),
        ..first.clone()
    };
    let second_now = Item {
        code: "code-1".to_string(),
        ..second.clone()
    };
    for record in [&first_now, &second_now] {
        assert_eq!(
            fetch(
                &engine,
                items.read_unique(ITEM_BY_CODE, code_values(record), Guard::None)
            )
            .await,
            Ok(record.clone())
        );
    }
    assert_eq!(
        fetch(
            &engine,
            items.read_unique_maybe(ITEM_BY_CODE, code_values(&second), Guard::None)
        )
        .await,
        Ok(None)
    );
    assert_eq!(get_raw(&engine, &code_key(&second)).await, None);
}

#[tokio::test]
async fn swapping_two_unique_values_in_one_transaction_needs_a_temporary_value() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let first = item(&scope, 1, "code-1");
    let second = item(&scope, 2, "code-2");
    create(&items, &[first.clone(), second.clone()])
        .await
        .unwrap();

    assert_eq!(
        recode(
            &items,
            &[
                (first.id.as_str(), "code-2"),
                (second.id.as_str(), "code-1")
            ],
        )
        .await,
        Err(violation(&scope, "code-2"))
    );
    for record in [&first, &second] {
        assert_eq!(
            fetch(
                &engine,
                items.read_unique(ITEM_BY_CODE, code_values(record), Guard::None)
            )
            .await,
            Ok(record.clone())
        );
    }

    recode(
        &items,
        &[
            (first.id.as_str(), "code-temporary"),
            (second.id.as_str(), "code-1"),
            (first.id.as_str(), "code-2"),
        ],
    )
    .await
    .unwrap();
    let first_now = Item {
        code: "code-2".to_string(),
        ..first.clone()
    };
    let second_now = Item {
        code: "code-1".to_string(),
        ..second.clone()
    };
    for record in [&first_now, &second_now] {
        assert_eq!(
            fetch(
                &engine,
                items.read_unique(ITEM_BY_CODE, code_values(record), Guard::None)
            )
            .await,
            Ok(record.clone())
        );
    }
    assert_eq!(
        get_raw(
            &engine,
            &format!("index:{ITEM_BY_CODE}:{scope}:code-temporary:")
        )
        .await,
        None
    );
}

#[tokio::test]
async fn an_update_rewrites_the_copies_and_moves_its_entries_and_a_remove_deletes_them_all() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let record = Item {
        tags: vec!["kept".to_string(), "dropped".to_string()],
        ..item(&scope, 1, "code-1")
    };
    create(&items, &[record.clone()]).await.unwrap();
    let summary = get_raw(&engine, &summary_key(&record)).await.unwrap();

    let edited = Item {
        body: "edited".to_string(),
        ..record.clone()
    };
    replace(&items, &record, &edited).await.unwrap();
    let stored = get_raw(&engine, &record_key(&edited)).await.unwrap();
    assert_eq!(serde_json::from_slice::<Item>(&stored).unwrap(), edited);
    assert_eq!(
        get_raw(&engine, &rank_key(&edited)).await,
        Some(stored.clone())
    );
    assert_eq!(get_raw(&engine, &code_key(&edited)).await, Some(stored));
    assert_eq!(get_raw(&engine, &summary_key(&edited)).await, Some(summary));

    let moved = Item {
        rank: 2,
        code: "code-2".to_string(),
        tags: vec!["kept".to_string(), "added".to_string()],
        contact: Contact {
            email: "moved@example.com".to_string(),
            phone: edited.contact.phone.clone(),
        },
        ..edited.clone()
    };
    replace(&items, &edited, &moved).await.unwrap();
    for gone in [
        rank_key(&edited),
        code_key(&edited),
        summary_key(&edited),
        tag_key(&edited, "dropped"),
    ] {
        assert_eq!(get_raw(&engine, &gone).await, None, "{gone}");
    }
    let stored = get_raw(&engine, &record_key(&moved)).await.unwrap();
    assert_eq!(
        get_raw(&engine, &rank_key(&moved)).await,
        Some(stored.clone())
    );
    assert_eq!(get_raw(&engine, &code_key(&moved)).await, Some(stored));
    let part = get_raw(&engine, &summary_key(&moved)).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&part).unwrap(),
        json!([
            moved.id,
            {
                "id": moved.id,
                "rank": 2,
                "contact": { "email": "moved@example.com" },
                "lines": [{ "sku": "code-1-first" }, { "sku": "code-1-second" }]
            }
        ])
    );
    let mut tags = vec![tag_key(&moved, "kept"), tag_key(&moved, "added")];
    tags.sort();
    assert_eq!(
        keys_with_prefix(&engine, &format!("index:{ITEM_BY_TAG}:{scope}:")).await,
        tags
    );

    remove_record(&items, &moved.id).await.unwrap();
    assert_eq!(get_raw(&engine, &record_key(&moved)).await, None);
    for index in [ITEM_BY_RANK, ITEM_BY_CODE, ITEM_SUMMARY, ITEM_BY_TAG] {
        assert!(
            keys_with_prefix(&engine, &format!("index:{index}:{scope}:"))
                .await
                .is_empty(),
            "{index}"
        );
    }
}

#[tokio::test]
async fn index_definitions_that_break_a_rule_are_refused_before_anything_is_written() {
    let engine = native_engine().await;
    let refused = engine.clone().model::<Refused>();
    let scope = fresh_scope();
    let records = [
        Rule::NoField,
        Rule::EmptyText,
        Rule::UniqueKeysOnly,
        Rule::UniquePart,
        Rule::EmptyPart,
        Rule::EmptyName,
        Rule::NameWithColon,
        Rule::NameWithBackslash,
    ]
    .iter()
    .map(|rule| Refused {
        id: fresh_id(),
        scope: scope.clone(),
        rule: *rule,
    })
    .collect::<Vec<_>>();
    let outcomes = engine
        .transaction(|tx| {
            let records = records.clone();
            let refused = refused.clone();
            async move {
                let mut outcomes = Vec::new();
                for record in &records {
                    outcomes.push(refused.create(record, &tx).await);
                }
                Ok::<_, TitoError>(outcomes)
            }
        })
        .await
        .unwrap();
    for (record, outcome) in records.iter().zip(outcomes) {
        assert!(
            matches!(outcome, Err(TitoError::IndexError(_))),
            "{:?}: {outcome:?}",
            record.rule
        );
        assert_eq!(
            get_raw(&engine, &format!("table:{REFUSED}:{}", record.id)).await,
            None
        );
    }
    assert!(
        keys_with_prefix(&engine, &format!("index:{REFUSED_BY_SCOPE}:{scope}:"))
            .await
            .is_empty()
    );

    let kept = Refused {
        id: fresh_id(),
        scope: scope.clone(),
        rule: Rule::Kept,
    };
    create(&refused, &[kept.clone()]).await.unwrap();
    let outcome = engine
        .transaction(|tx| {
            let refused = refused.clone();
            let id = kept.id.clone();
            async move {
                let old = tx.batch_get(refused.read(&id, Guard::None)).await?;
                let new = Refused {
                    rule: Rule::EmptyText,
                    ..old.clone()
                };
                Ok::<_, TitoError>(refused.update(&old, &new, &tx).await)
            }
        })
        .await
        .unwrap();
    assert!(matches!(outcome, Err(TitoError::IndexError(_))));
    assert_eq!(
        fetch(&engine, refused.read(&kept.id, Guard::None)).await,
        Ok(kept.clone())
    );
    assert_eq!(
        keys_with_prefix(&engine, &format!("index:{REFUSED_BY_SCOPE}:{scope}:")).await,
        vec![format!("index:{REFUSED_BY_SCOPE}:{scope}:{}:", kept.id)]
    );

    let mut empty = refused.query_by_index(REFUSED_BY_SCOPE);
    empty.value(text(""));
    assert!(matches!(
        empty.execute_ids(None).await,
        Err(TitoError::IndexError(_))
    ));
    let mut misnamed = refused.query_by_index("lib-index:refused");
    misnamed.value(text(&scope));
    assert!(matches!(
        misnamed.execute_ids(None).await,
        Err(TitoError::IndexError(_))
    ));
}

#[tokio::test]
async fn scan_notes_answer_beside_key_notes_in_one_batch_and_their_records_count_as_read() {
    let engine = native_engine().await;
    let items = engine.clone().model::<Item>();
    let scope = fresh_scope();
    let first = Item {
        tags: vec!["blue".to_string()],
        ..item(&scope, 1, "code-1")
    };
    let second = item(&scope, 2, "code-2");
    create(&items, &[first.clone(), second.clone()])
        .await
        .unwrap();

    let mut ranks = rank_query(&items, &scope);
    ranks.limit(Some(1));
    let mut summaries = items.query_by_index(ITEM_SUMMARY);
    summaries.value(text(&scope)).descending();
    let mut tags = items.query_by_index(ITEM_BY_TAG);
    tags.value(text(&scope));
    let batch = (
        items.read(&second.id, Guard::None),
        ranks.read(),
        summaries.read_part::<ItemSummary>(),
        tags.read_ids(),
        items.read_unique(ITEM_BY_CODE, code_values(&second), Guard::None),
        items.read_maybe(&fresh_id(), Guard::None),
    );
    let (first_ref, second_ref, items_ref) = (&first, &second, &items);
    let renamed = engine
        .transaction(move |tx| async move {
            let (by_id, page, parts, ids, by_code, missing) = tx.batch_get(batch).await?;
            assert_eq!(by_id, *second_ref);
            assert_eq!(page.items, vec![first_ref.clone()]);
            assert!(page.cursor.is_some());
            assert_eq!(
                parts.items,
                vec![summary_of(second_ref), summary_of(first_ref)]
            );
            assert_eq!(ids.items, vec![first_ref.id.clone()]);
            assert_eq!(by_code, *second_ref);
            assert_eq!(missing, None);
            let listed = page.items[0].clone();
            let renamed = Item {
                body: "renamed".to_string(),
                ..listed.clone()
            };
            items_ref.update(&listed, &renamed, &tx).await?;
            Ok::<_, TitoError>(renamed)
        })
        .await
        .unwrap();
    assert_eq!(
        fetch(&engine, items.read(&first.id, Guard::None)).await,
        Ok(renamed)
    );
}

#[tokio::test]
async fn rebuild_index_writes_every_entry_again_from_the_records() {
    let engine = native_engine().await;
    let rebuilt = engine.clone().model::<Rebuilt>();
    let group = fresh_scope();
    let records = (0..3)
        .map(|rank| Rebuilt {
            id: fresh_id(),
            group: group.clone(),
            code: format!("{group}-{rank}"),
            rank,
        })
        .collect::<Vec<_>>();
    create(&rebuilt, &records).await.unwrap();
    let ghost = Rebuilt {
        id: fresh_id(),
        group: group.clone(),
        code: format!("{group}-ghost"),
        rank: 9,
    };
    let wrong = Rebuilt {
        rank: 7,
        ..records[0].clone()
    };
    put_raw(
        &engine,
        &group_key(&records[0]),
        serde_json::to_vec(&wrong).unwrap(),
    )
    .await;
    put_raw(
        &engine,
        &group_key(&ghost),
        serde_json::to_vec(&ghost).unwrap(),
    )
    .await;
    delete_raw(
        &engine,
        vec![group_key(&records[1]), rebuilt_key(&records[2])],
    )
    .await;

    rebuilt.rebuild_index(REBUILT_BY_GROUP).await.unwrap();

    let mut expected = vec![group_key(&records[0]), group_key(&records[1])];
    expected.sort();
    assert_eq!(
        keys_with_prefix(&engine, &format!("index:{REBUILT_BY_GROUP}:{group}:")).await,
        expected
    );
    for record in &records[..2] {
        let stored = get_raw(&engine, &rebuilt_key(record)).await.unwrap();
        assert_eq!(get_raw(&engine, &group_key(record)).await, Some(stored));
    }
    assert!(get_raw(&engine, &rebuilt_code_key(&records[2]))
        .await
        .is_some());

    rebuilt.rebuild_index(REBUILT_BY_CODE).await.unwrap();

    assert_eq!(get_raw(&engine, &rebuilt_code_key(&records[2])).await, None);
    for record in &records[..2] {
        let stored = get_raw(&engine, &rebuilt_key(record)).await.unwrap();
        assert_eq!(
            get_raw(&engine, &rebuilt_code_key(record)).await,
            Some(stored)
        );
    }
    let mut query = rebuilt.query_by_index(REBUILT_BY_GROUP);
    query.value(text(&group));
    assert_eq!(
        query.execute(None).await.unwrap().items,
        records[..2].to_vec()
    );
}

#[tokio::test]
async fn rebuild_index_refuses_a_unique_value_two_records_claim() {
    let engine = native_engine().await;
    let claimed = engine.clone().model::<Claimed>();
    let code = fresh_scope();
    let holder = Claimed {
        id: fresh_id(),
        code: code.clone(),
    };
    let claimant = Claimed {
        id: fresh_id(),
        code: code.clone(),
    };
    create(&claimed, &[holder.clone()]).await.unwrap();
    let holder_key = format!("table:{CLAIMED}:{}", holder.id);
    let claimant_key = format!("table:{CLAIMED}:{}", claimant.id);
    put_raw(
        &engine,
        &claimant_key,
        serde_json::to_vec(&claimant).unwrap(),
    )
    .await;

    assert_eq!(
        claimed.rebuild_index(CLAIMED_BY_CODE).await,
        Err(TitoError::UniqueViolation {
            model: CLAIMED.to_string(),
            index: CLAIMED_BY_CODE.to_string(),
            values: vec![text(&code)],
        })
    );
    delete_raw(&engine, vec![holder_key, claimant_key]).await;
}
