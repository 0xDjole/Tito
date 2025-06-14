use crate::{
    error::TitoError,
    transaction::TitoTransaction,
    types::{StorageEngine, TitoEmbeddedRelationshipConfig, TitoModelTrait},
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;

impl<
        E: StorageEngine,
        T: Default
            + Clone
            + Serialize
            + DeserializeOwned
            + Unpin
            + std::marker::Send
            + Sync
            + TitoModelTrait,
    > TitoModel<E, T>
{
    pub fn stitch_relationship(
        &self,
        item: &mut Value,
        rel_map: &HashMap<String, Value>,
        config: &TitoEmbeddedRelationshipConfig,
    ) {
        let source_parts: Vec<&str> = config.source_field_name.split('.').collect();
        let dest_parts: Vec<&str> = config.destination_field_name.split('.').collect();

        Self::_stitch_recursive_helper(item, &source_parts, &dest_parts, rel_map, config);
    }

    fn _stitch_recursive_helper(
        current_json_node: &mut Value,
        source_path_remaining: &[&str],
        dest_path_remaining: &[&str],
        rel_map: &HashMap<String, Value>,
        config: &TitoEmbeddedRelationshipConfig,
    ) {
        if source_path_remaining.len() == 1 && dest_path_remaining.len() == 1 {
            let source_key = source_path_remaining[0];
            let dest_key = dest_path_remaining[0];

            if let Some(obj_to_modify) = current_json_node.as_object_mut() {
                if let Some(id_val_at_source_key) = obj_to_modify.get(source_key) {
                    if let Some(id_str) = id_val_at_source_key.as_str() {
                        let rel_lookup_key = format!("table:{}:{}", config.model, id_str);
                        if let Some(related_data) = rel_map.get(&rel_lookup_key) {
                            obj_to_modify.insert(dest_key.to_string(), related_data.clone());
                        }
                    } else if let Some(ids_array) = id_val_at_source_key.as_array() {
                        // Source is an array of ID strings
                        let mut stitched_related_items_array = Vec::new();
                        for id_elem_in_array in ids_array {
                            if let Some(id_str_elem) = id_elem_in_array.as_str() {
                                let rel_lookup_key =
                                    format!("table:{}:{}", config.model, id_str_elem);
                                if let Some(related_data) = rel_map.get(&rel_lookup_key) {
                                    stitched_related_items_array.push(related_data.clone());
                                }
                            }
                        }

                        obj_to_modify.insert(
                            dest_key.to_string(),
                            Value::Array(stitched_related_items_array),
                        );
                    }
                }
            }
            return;
        }

        if !source_path_remaining.is_empty()
            && !dest_path_remaining.is_empty()
            && source_path_remaining[0] == dest_path_remaining[0]
        {
            let common_key = source_path_remaining[0];
            if let Some(next_json_node_candidate) = current_json_node.get_mut(common_key) {
                match next_json_node_candidate {
                    Value::Object(obj) => {
                        Self::_stitch_recursive_helper(
                            next_json_node_candidate, // obj is implicitly &mut Value here
                            &source_path_remaining[1..],
                            &dest_path_remaining[1..],
                            rel_map,
                            config,
                        );
                    }
                    Value::Array(arr) => {
                        for element_in_array in arr.iter_mut() {
                            Self::_stitch_recursive_helper(
                                element_in_array,
                                &source_path_remaining[1..],
                                &dest_path_remaining[1..],
                                rel_map,
                                config,
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn get_relationship_data(
        &self,
        items: &Vec<(String, Value)>,
        rels_config: &[TitoEmbeddedRelationshipConfig],
        rels: &Vec<String>, // List of destination_field_names to populate
    ) -> Vec<(TitoEmbeddedRelationshipConfig, String)> {
        // (Config for this rel, "model_name:id_found")
        let mut relationship_keys_to_fetch = Vec::new();

        for (_, item_value) in items {
            for config in rels_config {
                // Check if this relationship is requested
                if rels.contains(&config.destination_field_name) {
                    // Use get_nested_values to find all ID values at the source path
                    if let Some(found_values_at_source_path) =
                        self.get_nested_values(item_value, &config.source_field_name)
                    {
                        for value_or_array_of_values in found_values_at_source_path {
                            // The value_or_array_of_values could be a single ID (Value::String)
                            // or an array of IDs (Value::Array of Value::String)
                            if let Some(id_str) = value_or_array_of_values.as_str() {
                                if id_str != "__null__" {
                                    // Assuming __null__ is a skip marker
                                    relationship_keys_to_fetch.push((
                                        config.clone(),
                                        format!("table:{}:{}", config.model, id_str),
                                    ));
                                }
                            } else if let Some(id_array) = value_or_array_of_values.as_array() {
                                for id_element in id_array {
                                    if let Some(id_str) = id_element.as_str() {
                                        if id_str != "__null__" {
                                            relationship_keys_to_fetch.push((
                                                config.clone(),
                                                format!("table:{}:{}", config.model, id_str),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        relationship_keys_to_fetch
    }

    pub async fn fetch_and_stitch_relationships(
        &self,
        items: Vec<(String, Value)>,
        rels: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        if rels.is_empty() {
            return Ok(items);
        }

        let rels_config = self.get_embedded_relationships();
        let relationship_data = self.get_relationship_data(&items, &rels_config, &rels);

        let rel_keys: Vec<String> = relationship_data.into_iter().map(|item| item.1).collect();

        let rel_items = self.batch_get(rel_keys.clone(), 10, 2, tx).await?;

        let mut rel_map: HashMap<String, Value> = HashMap::new();
        for kv in rel_items {
            rel_map.insert(kv.0, kv.1);
        }

        let final_items = items
            .into_iter()
            .map(|mut item| {
                for config in &rels_config {
                    if rels.contains(&config.destination_field_name) {
                        self.stitch_relationship(&mut item.1, &rel_map, config);
                    }
                }
                item
            })
            .collect();

        Ok(final_items)
    }

    pub fn extract_relationship(&self, input: &str) -> Option<String> {
        if let (Some(start), Some(end)) = (input.find("-").map(|idx| idx + 1), input.rfind("-")) {
            if start < end {
                return Some(input[start..end].trim().to_string());
            }
        }

        None
    }
}
