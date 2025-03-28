extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, RegexLineModifier};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_regex_line_modifier_new() {
        // Test with default values
        let config = json!({});
        let result = RegexLineModifier::new(&config);
        assert!(result.is_ok());
        
        let modifier = result.unwrap();
        assert_eq!(modifier.text_field, "text");
        assert!(modifier.regex.is_match("10K likes"));
        assert!(modifier.regex.is_match("5.3M views"));
        assert!(!modifier.regex.is_match("normal text"));

        // Test with custom values
        let config = json!({
            "text_field": "content",
            "regex": r"^test\d+$"
        });
        let result = RegexLineModifier::new(&config);
        assert!(result.is_ok());
        
        let modifier = result.unwrap();
        assert_eq!(modifier.text_field, "content");
        assert_eq!(modifier.regex_string, r"^test\d+$");
        assert!(modifier.regex.is_match("test123"));
        assert!(!modifier.regex.is_match("test"));
    }

    #[test]
    fn test_process_with_matching_lines() {
        let config = json!({});
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        let data = json!({
            "text": "This is a normal line\n10K likes\nAnother normal line\n5.2M views\nFinal normal line"
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        let processed_text = processed["text"].as_str().unwrap();
        assert_eq!(processed_text, "This is a normal line\nAnother normal line\nFinal normal line");
        assert!(!processed_text.contains("10K likes"));
        assert!(!processed_text.contains("5.2M views"));
    }

    #[test]
    fn test_process_with_no_matching_lines() {
        let config = json!({});
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        let data = json!({
            "text": "This is a normal line\nAnother normal line\nFinal normal line"
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        let processed_text = processed["text"].as_str().unwrap();
        assert_eq!(processed_text, "This is a normal line\nAnother normal line\nFinal normal line");
    }

    #[test]
    fn test_process_with_all_matching_lines() {
        let config = json!({});
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        let data = json!({
            "text": "10K likes\n5.2M views\n3B followers"
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_process_with_custom_regex() {
        let config = json!({
            "regex": r"^remove:"
        });
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        let data = json!({
            "text": "Keep this line\nremove: this line\nKeep this one too\nremove: another line"
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        let processed_text = processed["text"].as_str().unwrap();
        assert_eq!(processed_text, "Keep this line\nKeep this one too");
    }

    #[test]
    fn test_process_with_custom_field() {
        let config = json!({
            "text_field": "content"
        });
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        let data = json!({
            "content": "This is a normal line\n10K likes\nAnother normal line"
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        let processed_text = processed["content"].as_str().unwrap();
        assert_eq!(processed_text, "This is a normal line\nAnother normal line");
    }

    #[test]
    fn test_case_insensitivity() {
        let config = json!({});
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        let data = json!({
            "text": "This is a normal line\n10k Likes\nAnother normal line\n5.2m Views"
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        let processed = result.unwrap();
        let processed_text = processed["text"].as_str().unwrap();
        assert_eq!(processed_text, "This is a normal line\nAnother normal line");
    }

    #[test]
    fn test_edge_cases() {
        let config = json!({});
        let modifier = RegexLineModifier::new(&config).unwrap();
        
        // Empty input
        let data = json!({
            "text": ""
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        // All spaces
        let data = json!({
            "text": "   \n   \n   "
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_some());
        
        // Various formats of social media metrics
        let data = json!({
            "text": "1k likes\n1.5K likes\n10,000 followers\n(10M views)\n 5B downloads "
        });
        
        let result = modifier.process(data).unwrap();
        assert!(result.is_none());
    }
}