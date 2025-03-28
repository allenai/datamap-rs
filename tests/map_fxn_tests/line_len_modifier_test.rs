extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, LineLenModifier};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_with_defaults() {
        let config = json!({});
        let modifier = LineLenModifier::new(&config).unwrap();
        
        assert_eq!(modifier.text_field, "text");
        assert_eq!(modifier.lower_bound, 0);
    }

    #[test]
    fn test_new_with_custom_values() {
        let config = json!({
            "text_field": "content",
            "lower_bound": 5
        });
        let modifier = LineLenModifier::new(&config).unwrap();
        
        assert_eq!(modifier.text_field, "content");
        assert_eq!(modifier.lower_bound, 5);
    }

    #[test]
    fn test_process_empty_text() {
        let modifier = LineLenModifier {
            text_field: "text".to_string(),
            lower_bound: 1
        };
        
        let data = json!({ "text": "" });
        let result = modifier.process(data).unwrap();
        
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_no_passing_lines() {
        let modifier = LineLenModifier {
            text_field: "text".to_string(),
            lower_bound: 3
        };
        
        let data = json!({ "text": "hello\nhi there" });
        let result = modifier.process(data).unwrap();
        
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_all_lines_pass() {
        let modifier = LineLenModifier {
            text_field: "text".to_string(),
            lower_bound: 2
        };
        
        let data = json!({ "text": "hello world\nrust is awesome" });
        let result = modifier.process(data).unwrap();
        
        assert!(result.is_some());
        let output = result.unwrap();
        assert_eq!(output["text"], "hello world\nrust is awesome");
    }

    #[test]
    fn test_process_some_lines_pass() {
        let modifier = LineLenModifier {
            text_field: "text".to_string(),
            lower_bound: 3
        };
        
        let data = json!({ 
            "text": "hello world\nrust is awesome\nshort line",
            "id": 123
        });
        let result = modifier.process(data).unwrap();
        
        assert!(result.is_some());
        let output = result.unwrap();
        assert_eq!(output["text"], "rust is awesome");
        assert_eq!(output["id"], 123);
    }

    #[test]
    fn test_process_custom_field_name() {
        let modifier = LineLenModifier {
            text_field: "content".to_string(),
            lower_bound: 2
        };
        
        let data = json!({ 
            "content": "hello world\nsingle",
            "metadata": { "source": "test" }
        });
        let result = modifier.process(data).unwrap();
        
        assert!(result.is_some());
        let output = result.unwrap();
        assert_eq!(output["content"], "hello world");
        assert_eq!(output["metadata"]["source"], "test");
    }

    #[test]
    fn test_process_unicode_words() {
        let modifier = LineLenModifier {
            text_field: "text".to_string(),
            lower_bound: 3
        };
        
        let data = json!({ "text": "こんにちは 世界 rust\nHello world" });
        let result = modifier.process(data).unwrap();
        
        assert!(result.is_some());
        let output = result.unwrap();
        assert_eq!(output["text"], "こんにちは 世界 rust");
    }

    #[test]
    fn test_process_zero_lower_bound() {
        let modifier = LineLenModifier {
            text_field: "text".to_string(),
            lower_bound: 0
        };
        
        let data = json!({ "text": "hello\n\nempty line" });
        let result = modifier.process(data).unwrap();
        
        assert!(result.is_some());
        let output = result.unwrap();
        assert_eq!(output["text"], "hello\n\nempty line");
    }

 
}