extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, StringSubModifier};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // Helper function to create a test configuration
    fn create_test_config(field: &str, substitutions: Vec<Vec<&str>>) -> Value {
        let mut subs = Vec::new();
        for sub in substitutions {
            subs.push(json!(sub));
        }
        
        json!({
            "text_field": field,
            "subs": subs
        })
    }

    #[test]
    fn test_new_valid_config() {
        let config = create_test_config("content", vec![
            vec!["hello", "hi"],
            vec!["world", "earth"]
        ]);
        
        let result = StringSubModifier::new(&config);
        assert!(result.is_ok());
        
        let filter = result.unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.subs.len(), 2);
        assert_eq!(filter.subs[0].0, "hello");
        assert_eq!(filter.subs[0].1, "hi");
        assert_eq!(filter.subs[1].0, "world");
        assert_eq!(filter.subs[1].1, "earth");
    }

    #[test]
    fn test_new_empty_subs() {
        let config = create_test_config("content", vec![]);
        
        let result = StringSubModifier::new(&config);
        assert!(result.is_ok());
        
        let filter = result.unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.subs.len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_new_missing_text_field() {
        let config = json!({
            "subs": json!([["hello", "hi"]])
        });
        
        StringSubModifier::new(&config).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_new_missing_subs() {
        let config = json!({
            "text_field": "content"
        });
        
        StringSubModifier::new(&config).unwrap();
    }

    #[test]
    fn test_process_single_substitution() {
        let config = create_test_config("text", vec![
            vec!["old", "new"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "text": "This is an old text",
            "other_field": "unchanged"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed["text"], "This is an new text");
        assert_eq!(processed["other_field"], "unchanged");
    }

    #[test]
    fn test_process_multiple_substitutions() {
        let config = create_test_config("message", vec![
            vec!["hello", "hi"],
            vec!["world", "earth"],
            vec!["!", "!!"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "message": "hello world!",
            "other_field": "unchanged"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed["message"], "hi earth!!");
        assert_eq!(processed["other_field"], "unchanged");
    }

    #[test]
    fn test_process_nested_field() {
        let config = create_test_config("nested.text", vec![
            vec!["test", "example"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "nested": {
                "text": "This is a test"
            }
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed["nested"]["text"], "This is a example");
    }

    #[test]
    fn test_process_no_substitutions_needed() {
        let config = create_test_config("content", vec![
            vec!["not-present", "replacement"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "content": "This text has no matches"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed["content"], "This text has no matches");
    }

    #[test]
    fn test_process_multiple_occurrences() {
        let config = create_test_config("text", vec![
            vec!["a", "X"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "text": "a banana and an apple"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed["text"], "X bXnXnX Xnd Xn Xpple");
    }

    #[test]
    fn test_process_chained_substitutions() {
        // Test that substitutions are applied in order
        let config = create_test_config("text", vec![
            vec!["abc", "123"],
            vec!["123", "xyz"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "text": "abc"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed["text"], "xyz");
    }

    #[test]
    #[should_panic]
    fn test_process_missing_field() {
        let config = create_test_config("missing_field", vec![
            vec!["a", "b"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "text": "This doesn't have the right field"
        });
        
        filter.process(data).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_process_non_string_field() {
        let config = create_test_config("number", vec![
            vec!["a", "b"]
        ]);
        
        let filter = StringSubModifier::new(&config).unwrap();
        let data = json!({
            "number": 42
        });
        
        filter.process(data).unwrap();
    }
}