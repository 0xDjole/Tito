use crate::{
    error::TitoError,
    types::{TitoEmbeddedRelationshipConfig, TitoEngine, TitoModelTrait},
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;

impl<
        E: TitoEngine,
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
    /// Public entry point for stitching a relationship into a JSON Value.
    pub fn stitch_relationship(
        &self,
        item: &mut Value,
        rel_map: &HashMap<String, Value>,
        config: &TitoEmbeddedRelationshipConfig,
    ) {
        let source_parts: Vec<&str> = config.source_field_name.split('.').collect();
        let dest_parts: Vec<&str> = config.destination_field_name.split('.').collect();
        // Calls the robust iterative helper function.
        Self::_stitch_iterative_helper(item, &source_parts, &dest_parts, rel_map, config);
    }

    /// Iteratively traverses a JSON structure to embed related data, avoiding recursion.
    fn _stitch_iterative_helper(
        root_node: &mut Value,
        source_path: &[&str],
        dest_path: &[&str],
        rel_map: &HashMap<String, Value>,
        config: &TitoEmbeddedRelationshipConfig,
    ) {
        // Use a stack to manage traversal, mimicking a recursive call stack safely.
        let mut stack: Vec<(&mut Value, &[&str], &[&str])> = Vec::new();
        stack.push((root_node, source_path, dest_path));

        while let Some((current_node, source_path_remaining, dest_path_remaining)) = stack.pop() {
            // Base case: We've reached the target object for stitching.
            if source_path_remaining.len() == 1 && dest_path_remaining.len() == 1 {
                let source_key = source_path_remaining[0];
                let dest_key = dest_path_remaining[0];

                if let Some(obj_to_modify) = current_node.as_object_mut() {
                    if let Some(id_val_at_source_key) = obj_to_modify.get(source_key) {
                        // Handle a single ID string.
                        if let Some(id_str) = id_val_at_source_key.as_str() {
                            let rel_lookup_key = format!("table:{}:{}", config.model, id_str);
                            if let Some(related_data) = rel_map.get(&rel_lookup_key) {
                                obj_to_modify.insert(dest_key.to_string(), related_data.clone());
                            }
                        // Handle an array of ID strings.
                        } else if let Some(ids_array) = id_val_at_source_key.as_array() {
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
                continue; // Finished with this branch, process next item on stack.
            }

            // Traversal step: Navigate deeper if paths have a common prefix.
            if !source_path_remaining.is_empty()
                && !dest_path_remaining.is_empty()
                && source_path_remaining[0] == dest_path_remaining[0]
            {
                let common_key = source_path_remaining[0];
                if let Some(next_node_candidate) = current_node.get_mut(common_key) {
                    match next_node_candidate {
                        Value::Object(_) => {
                            stack.push((
                                next_node_candidate,
                                &source_path_remaining[1..],
                                &dest_path_remaining[1..],
                            ));
                        }
                        Value::Array(arr) => {
                            // If it's an array, push each element onto the stack to be processed.
                            for element_in_array in arr.iter_mut() {
                                stack.push((
                                    element_in_array,
                                    &source_path_remaining[1..],
                                    &dest_path_remaining[1..],
                                ));
                            }
                        }
                        _ => {} // Not an object or array, cannot traverse further down this path.
                    }
                }
            }
        }
    }

    /// Gathers all relationship keys that need to be fetched from a list of items.
    pub fn get_relationship_data(
        &self,
        items: &Vec<(String, Value)>,
        rels_config: &[TitoEmbeddedRelationshipConfig],
        rels: &Vec<String>, // List of destination_field_names to populate
    ) -> Vec<(TitoEmbeddedRelationshipConfig, String)> {
        let mut relationship_keys_to_fetch = Vec::new();

        for (_, item_value) in items {
            for config in rels_config {
                // Check if this relationship is requested for population.
                if rels.contains(&config.destination_field_name) {
                    if let Some(found_values_at_source_path) =
                        self.get_nested_values(item_value, &config.source_field_name)
                    {
                        for value_or_array_of_values in found_values_at_source_path {
                            if let Some(id_str) = value_or_array_of_values.as_str() {
                                if id_str != "__null__" {
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

    /// Fetches and stitches all requested relationships for a vector of items.
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

        // Fetch all related items in a batch.
        let rel_items = self.batch_get(rel_keys, 10, 2, tx).await?;

        let mut rel_map: HashMap<String, Value> = HashMap::new();
        for kv in rel_items {
            rel_map.insert(kv.0, kv.1);
        }

        // Map over the original items and stitch the fetched data into each one.
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

    /// A helper function to extract a relationship string.
    pub fn extract_relationship(&self, input: &str) -> Option<String> {
        if let (Some(start), Some(end)) = (input.find('-').map(|idx| idx + 1), input.rfind('-')) {
            if start < end {
                return Some(input[start..end].trim().to_string());
            }
        }
        None
    }
}
