extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, StringEQFilter};


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // Helper function to create a test configuration
    fn create_test_config(field: &str, targets: Vec<&str>, case_sensitive: Option<bool>) -> Value {
        let mut config = json!({
            "text_field": field,
            "targets": targets
        });
        
        if let Some(cs) = case_sensitive {
            config["case_sensitive"] = json!(cs);
        }
        
        config
    }

    #[test]
    fn test_new_with_case_sensitive() {
        let config = create_test_config("content", vec!["hello", "world"], Some(true));
        
        let result = StringEQFilter::new(&config);
        assert!(result.is_ok());
        
        let filter = result.unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.targets, vec!["hello", "world"]);
        assert_eq!(filter.case_sensitive, true);
    }

    #[test]
    fn test_new_with_case_insensitive() {
        let config = create_test_config("content", vec!["Hello", "World"], Some(false));
        
        let result = StringEQFilter::new(&config);
        assert!(result.is_ok());
        
        let filter = result.unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.targets, vec!["hello", "world"]); // should be lowercase
        assert_eq!(filter.case_sensitive, false);
    }

    #[test]
    fn test_new_default_case_sensitive() {
        let config = create_test_config("content", vec!["Hello", "World"], None);
        
        let result = StringEQFilter::new(&config);
        assert!(result.is_ok());
        
        let filter = result.unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.targets, vec!["Hello", "World"]); // should remain as-is
        assert_eq!(filter.case_sensitive, true); // default value
    }

    #[test]
    fn test_new_empty_targets() {
        let config = create_test_config("content", vec![], Some(true));
        
        let result = StringEQFilter::new(&config);
        assert!(result.is_ok());
        
        let filter = result.unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.targets.len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_new_missing_text_field() {
        let config = json!({
            "targets": ["hello", "world"]
        });
        
        StringEQFilter::new(&config).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_new_missing_targets() {
        let config = json!({
            "text_field": "content"
        });
        
        StringEQFilter::new(&config).unwrap();
    }

    #[test]
    fn test_process_case_sensitive_match() {
        let config = create_test_config("text", vec!["World"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "text": "Hello World",
            "other_field": "unchanged"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed, data);
    }

    #[test]
    fn test_process_case_sensitive_no_match() {
        let config = create_test_config("text", vec!["world"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "text": "Hello World",
            "other_field": "unchanged"
        });
        
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_process_case_insensitive_match() {
        let config = create_test_config("text", vec!["world"], Some(false));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "text": "Hello World",
            "other_field": "unchanged"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed, data);
    }

    #[test]
    fn test_process_multiple_targets_one_matches() {
        let config = create_test_config("text", vec!["not-present", "World"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "text": "Hello World"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_process_multiple_targets_none_match() {
        let config = create_test_config("text", vec!["not-present", "also-not-present"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "text": "Hello World"
        });
        
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_process_nested_field() {
        let config = create_test_config("nested.text", vec!["example"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "nested": {
                "text": "This is an example"
            }
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed, data);
    }

    #[test]
    fn test_process_substring_match() {
        let config = create_test_config("content", vec!["part"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "content": "This contains the partial word"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        assert_eq!(processed, data);
    }

    #[test]
    fn test_process_empty_targets_no_match() {
        let config = create_test_config("content", vec![], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "content": "Any text here"
        });
        
        let result = filter.process(data).unwrap();
        assert!(result.is_none()); // No targets means no possible matches
    }

    #[test]
    #[should_panic]
    fn test_process_missing_field() {
        let config = create_test_config("missing_field", vec!["test"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "text": "This doesn't have the right field"
        });
        
        filter.process(data).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_process_non_string_field() {
        let config = create_test_config("number", vec!["42"], Some(true));
        
        let filter = StringEQFilter::new(&config).unwrap();
        let data = json!({
            "number": 42
        });
        
        filter.process(data).unwrap();
    }
}