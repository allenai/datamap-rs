extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, AlphabeticWordRatioFilter};


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_alphabetic_word_ratio_filter_new_default() {
        let config = json!({});
        let filter = AlphabeticWordRatioFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.max_ratio, f32::MAX);
    }

    #[test]
    fn test_alphabetic_word_ratio_filter_new_custom() {
        let config = json!({
            "text_field": "content",
            "max_ratio": 0.5
        });
        let filter = AlphabeticWordRatioFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.max_ratio, 0.5);
    }

    #[test]
    fn test_process_all_alphabetic() {
        let filter = AlphabeticWordRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.2,
        };
        
        let data = json!({
            "text": "This is all alphabetic text",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_process_mixed_content_below_threshold() {
        let filter = AlphabeticWordRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        // "123" and "456" are non-alphabetic (2 out of 5 words = 0.4 ratio)
        let data = json!({
            "text": "Some text 123 with 456",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_process_mixed_content_above_threshold() {
        let filter = AlphabeticWordRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.3,
        };
        
        // "123", "456", and "789" are non-alphabetic (3 out of 6 words = 0.5 ratio)
        let data = json!({
            "text": "Some text 123 456 789",
            "other_field": "value"
        });
        
        let result = filter.process(data).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_all_non_alphabetic() {
        let filter = AlphabeticWordRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        let data = json!({
            "text": "123 456 789",
            "other_field": "value"
        });
        
        let result = filter.process(data).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_empty_text() {
        let filter = AlphabeticWordRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        let data = json!({
            "text": "",
            "other_field": "value"
        });
        
        // This should handle the potential division by zero
        let result = filter.process(data).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_custom_field() {
        let filter = AlphabeticWordRatioFilter {
            text_field: String::from("content"),
            max_ratio: 0.5,
        };
        
        let data = json!({
            "content": "This is in a custom field",
            "text": "This would be ignored"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

}