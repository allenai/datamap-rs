extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, NewlineRemovalModifier};
#[cfg(test)]
mod tests {
	use super::*;
    use serde_json::json;
    

    #[test]
    fn test_newline_removal_modifier_new() {
        // Test with default values
        let config = json!({});
        let modifier = NewlineRemovalModifier::new(&config).unwrap();
        assert_eq!(modifier.text_field, "text");
        assert_eq!(modifier.max_consecutive, 2);

        // Test with custom values
        let config = json!({
            "text_field": "content",
            "max_consecutive": 3
        });
        let modifier = NewlineRemovalModifier::new(&config).unwrap();
        assert_eq!(modifier.text_field, "content");
        assert_eq!(modifier.max_consecutive, 3);
    }

    #[test]
    fn test_process_with_default_settings() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("text"),
            max_consecutive: 2,
        };

        // Test with text having excessive newlines
        let input = json!({
            "text": "Hello\n\n\n\nWorld"
        });
        
        let result = modifier.process(input).unwrap().unwrap();
        let processed_text = result["text"].as_str().unwrap();
        assert_eq!(processed_text, "Hello\n\nWorld");
    }

    #[test]
    fn test_process_with_custom_field_and_limit() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("content"),
            max_consecutive: 3,
        };

        // Test with text having excessive newlines
        let input = json!({
            "content": "Line 1\n\n\n\n\nLine 2\n\n\nLine 3"
        });
        
        let result = modifier.process(input).unwrap().unwrap();
        let processed_text = result["content"].as_str().unwrap();
        assert_eq!(processed_text, "Line 1\n\n\nLine 2\n\n\nLine 3");
    }

    #[test]
    fn test_process_with_no_excessive_newlines() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("text"),
            max_consecutive: 2,
        };

        // Test with text having no excessive newlines
        let input = json!({
            "text": "Hello\n\nWorld\nAgain"
        });
        
        let result = modifier.process(input).unwrap().unwrap();
        let processed_text = result["text"].as_str().unwrap();
        assert_eq!(processed_text, "Hello\n\nWorld\nAgain");
    }

    #[test]
    fn test_process_with_empty_text() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("text"),
            max_consecutive: 2,
        };

        // Test with empty text
        let input = json!({
            "text": ""
        });
        
        let result = modifier.process(input).unwrap().unwrap();
        let processed_text = result["text"].as_str().unwrap();
        assert_eq!(processed_text, "");
    }

    #[test]
    fn test_process_with_multiple_patterns() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("text"),
            max_consecutive: 1,
        };

        // Test with multiple patterns of excessive newlines
        let input = json!({
            "text": "Line 1\n\n\nLine 2\n\n\n\nLine 3\n\nLine 4"
        });
        
        let result = modifier.process(input).unwrap().unwrap();
        let processed_text = result["text"].as_str().unwrap();
        assert_eq!(processed_text, "Line 1\nLine 2\nLine 3\nLine 4");
    }

    #[test]
    fn test_process_preserves_other_fields() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("text"),
            max_consecutive: 2,
        };

        // Test that other fields in the JSON are preserved
        let input = json!({
            "text": "Hello\n\n\n\nWorld",
            "id": 123,
            "metadata": {
                "author": "Test User",
                "date": "2023-01-01"
            }
        });
        
        let result = modifier.process(input).unwrap().unwrap();
        
        // Check that text field was processed
        assert_eq!(result["text"].as_str().unwrap(), "Hello\n\nWorld");
        
        // Check that other fields were preserved
        assert_eq!(result["id"].as_i64().unwrap(), 123);
        assert_eq!(result["metadata"]["author"].as_str().unwrap(), "Test User");
        assert_eq!(result["metadata"]["date"].as_str().unwrap(), "2023-01-01");
    }

    #[test]
    fn test_process_with_missing_field() {
        let modifier = NewlineRemovalModifier {
            text_field: String::from("text"),
            max_consecutive: 2,
        };

        // Test with missing field - this would panic in a real scenario
        // but for testing we'll need to handle this differently
        let input = json!({
            "other_field": "value"
        });
        
        // In a complete implementation, we might expect an error here
        // or some fallback behavior, depending on requirements
        let result = std::panic::catch_unwind(|| {
            modifier.process(input)
        });
        
        assert!(result.is_err());
    }
}