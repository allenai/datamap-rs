extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, RatioLineModifier};


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_ratio_line_modifier_new() {
        // Test with uppercase check
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.5,
            "check": "uppercase"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        assert_eq!(modifier.text_field, "content");
        assert_eq!(modifier.upper_bound, 0.5);
        assert_eq!(modifier.check, "uppercase");
        
        // Test with numeric check
        let config = json!({
            "text_field": "text",
            "upper_bound": 0.3,
            "check": "numeric"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        assert_eq!(modifier.text_field, "text");
        assert_eq!(modifier.upper_bound, 0.3);
        assert_eq!(modifier.check, "numeric");
        
        // Test default text_field
        let config = json!({
            "upper_bound": 0.4,
            "check": "uppercase"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        assert_eq!(modifier.text_field, "text");  // Default value
        assert_eq!(modifier.upper_bound, 0.4);
        assert_eq!(modifier.check, "uppercase");
        
        // Test invalid check value
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.5,
            "check": "invalid"
        });
        
        let result = RatioLineModifier::new(&config);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_ratio_line_modifier_process_uppercase() {
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.3,
            "check": "uppercase"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        
        // Test with mixed case lines
        let data = json!({
            "content": "this is a lowercase line\nTHIS IS AN UPPERCASE LINE\nThis Has Some Uppercase Letters\nAnother 50% UPPERCASE line"
        });
        
        let result = modifier.process(data).unwrap().unwrap();
        let processed_text = result["content"].as_str().unwrap();
        // Only lines with <= 30% uppercase should remain
        assert!(processed_text.contains("this is a lowercase line"));
        assert!(!processed_text.contains("THIS IS AN UPPERCASE LINE"));
        assert!(processed_text.contains("This Has Some Uppercase Letters"));
        assert!(!processed_text.contains("Another 50% UPPERCASE line"));
    }
    
    #[test]
    fn test_ratio_line_modifier_process_numeric() {
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.2,
            "check": "numeric"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        
        // Test with lines containing different amounts of numeric characters
        let data = json!({
            "content": "This is a text without numbers\nThis has 1 number\nThis has 12345 numbers\nPhone: 555-123-4567"
        });
        
        let result = modifier.process(data).unwrap().unwrap();
        let processed_text = result["content"].as_str().unwrap();
        
        // Only lines with <= 20% numeric characters should remain
        assert!(processed_text.contains("This is a text without numbers"));
        assert!(processed_text.contains("This has 1 number"));
        assert!(!processed_text.contains("This has 12345 numbers"));
        assert!(!processed_text.contains("Phone: 555-123-4567"));
    }
    
    #[test]
    fn test_ratio_line_modifier_empty_lines() {
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.5,
            "check": "uppercase"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        
        // Test with empty lines
        let data = json!({
            "content": "\n\nThis is a normal line\n\nTHIS IS UPPERCASE\n"
        });
        
        let result = modifier.process(data).unwrap().unwrap();
        let processed_text = result["content"].as_str().unwrap();
        
        // Empty lines should always pass (0/0 is treated as 0 <= upper_bound)
        let expected_lines = vec!["", "", "This is a normal line", "", ""];
        let expected = expected_lines.join("\n");
        assert_eq!(processed_text, expected);
    }
    
    #[test]
    fn test_ratio_line_modifier_edge_cases() {
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.5,
            "check": "uppercase"
        });
        
        let modifier = RatioLineModifier::new(&config).unwrap();
        
        // Test with exact boundary cases
        let data = json!({
            "content": "HALF uppercase\nAAAaaa\nall lowercase"
        });
        
        let result = modifier.process(data.clone()).unwrap().unwrap();
        let processed_text = result["content"].as_str().unwrap();
        
        // "HALF uppercase" has exactly 4/14 ≈ 0.286 uppercase, which is <= 0.5
        assert!(processed_text.contains("HALF uppercase"));
        // "MORE than half UPPERCASE" has 12/24 = 0.5 uppercase, which is <= 0.5
        assert!(processed_text.contains("AAAaaa"));
        // "all lowercase" has 0 uppercase
        assert!(processed_text.contains("all lowercase"));
        
        // Test with 0.0 upper_bound (strict)
        let config = json!({
            "text_field": "content",
            "upper_bound": 0.0,
            "check": "uppercase"
        });
        
        let strict_modifier = RatioLineModifier::new(&config).unwrap();
        let result = strict_modifier.process(data).unwrap().unwrap();
        let processed_text = result["content"].as_str().unwrap();
        
        // Only lines with 0 uppercase should remain
        assert!(!processed_text.contains("HALF uppercase"));
        assert!(!processed_text.contains("AAAaaa"));
        assert!(processed_text.contains("all lowercase"));
    }
}