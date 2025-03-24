extern crate datamap_rs;
use serde_json::json;
//use datamap_rs::{DataProcessor};
use datamap_rs::map_fxn::{DataProcessor, AddIdModifier};
use uuid::Uuid;


#[test]
fn test_new_with_default_id_key() {
    // Test that the default id_key is "id" when not specified in config
    let config = json!({});
    let modifier = AddIdModifier::new(&config).unwrap();
    
    assert_eq!(modifier.id_key, "id");
}

#[test]
fn test_new_with_custom_id_key() {
    // Test that the id_key is taken from config when specified
    let config = json!({"id_key": "custom_id"});
    let modifier = AddIdModifier::new(&config).unwrap();
    
    assert_eq!(modifier.id_key, "custom_id");
}

#[test]
fn test_process_adds_id_to_empty_object() {
    // Test that process adds an id to an empty JSON object
    let modifier = AddIdModifier { id_key: String::from("id") };
    let data = json!({});
    
    let result = modifier.process(data).unwrap().unwrap();
    
    assert!(result.is_object());
    assert!(result.get("id").is_some());
    
    // Verify UUID format - should be a valid UUID string
    let id_str = result["id"].as_str().unwrap();
    assert!(Uuid::parse_str(id_str).is_ok());
}

#[test]
fn test_process_with_custom_id_key() {
    // Test that process adds an id using the custom id_key
    let modifier = AddIdModifier { id_key: String::from("custom_id") };
    let data = json!({"name": "test"});
    
    let result = modifier.process(data).unwrap().unwrap();
    
    assert!(result.is_object());
    assert_eq!(result["name"], "test");
    assert!(result.get("custom_id").is_some());
    
    // Verify UUID format
    let id_str = result["custom_id"].as_str().unwrap();
    assert!(Uuid::parse_str(id_str).is_ok());
}

#[test]
fn test_process_overwrites_existing_id() {
    // Test that process overwrites an existing id field
    let modifier = AddIdModifier { id_key: String::from("id") };
    let data = json!({"id": "old-id", "name": "test"});
    
    let result = modifier.process(data).unwrap().unwrap();
    
    assert!(result.is_object());
    assert_eq!(result["name"], "test");
    assert!(result.get("id").is_some());
    assert_ne!(result["id"], "old-id");
    
    // Verify UUID format
    let id_str = result["id"].as_str().unwrap();
    assert!(Uuid::parse_str(id_str).is_ok());
}

#[test]
fn test_process_with_nested_id_field() {
    // Test that process works with a nested path for id_key
    let modifier = AddIdModifier { id_key: String::from("metadata.id") };
    let data = json!({"name": "test", "metadata": {}});
    
    let result = modifier.process(data).unwrap().unwrap();
    
    assert!(result.is_object());
    assert_eq!(result["name"], "test");
    assert!(result["metadata"].is_object());
    assert!(result["metadata"].get("id").is_some());
    
    // Verify UUID format
    let id_str = result["metadata"]["id"].as_str().unwrap();
    assert!(Uuid::parse_str(id_str).is_ok());
}

#[test]
fn test_process_creates_nested_path() {
    // Test that process creates the nested path if it doesn't exist
    let modifier = AddIdModifier { id_key: String::from("metadata.nested.id") };
    let data = json!({"name": "test"});
    
    let result = modifier.process(data).unwrap().unwrap();
    
    assert!(result.is_object());
    assert_eq!(result["name"], "test");
    assert!(result["metadata"].is_object());
    assert!(result["metadata"]["nested"].is_object());
    assert!(result["metadata"]["nested"].get("id").is_some());
    
    // Verify UUID format
    let id_str = result["metadata"]["nested"]["id"].as_str().unwrap();
    assert!(Uuid::parse_str(id_str).is_ok());
}

