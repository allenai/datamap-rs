extern crate datamap_rs;

use datamap_rs::map_fxn::{DataProcessor, WordLenFilter};


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_word_length_filter_new_with_defaults() {
        let config = json!({});
        let filter = WordLenFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.lower_bound, 0.0);
        assert_eq!(filter.upper_bound, f32::MAX);
    }

    #[test]
    fn test_word_length_filter_new_with_custom_config() {
        let config = json!({
            "text_field": "content",
            "lower_bound": 3.5,
            "upper_bound": 6.0
        });
        let filter = WordLenFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.lower_bound, 3.5);
        assert_eq!(filter.upper_bound, 6.0);
    }

    #[test]
    fn test_process_within_bounds() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 3.0,
            upper_bound: 5.0,
        };
        
        // Average word length: (4 + 4 + 4) / 3 = 4.0
        let data = json!({
            "text": "test word here",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_process_below_lower_bound() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 4.0,
            upper_bound: 10.0,
        };
        
        // Average word length: (2 + 2 + 1) / 3 = 1.67
        let data = json!({
            "text": "it is a",
            "other_field": "value"
        });
        
        let result = filter.process(data).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_above_upper_bound() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 1.0,
            upper_bound: 4.0,
        };
        
        // Average word length: (11 + 7) / 2 = 9.0
        let data = json!({
            "text": "complicated extraordinary",
            "other_field": "value"
        });
        
        let result = filter.process(data).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_process_exact_bounds() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 5.0,
            upper_bound: 5.0,
        };
        
        // Average word length: (5 + 5 + 5) / 3 = 5.0
        let data = json!({
            "text": "hello world hello",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_process_custom_field_name() {
        let filter = WordLenFilter {
            text_field: String::from("content"),
            lower_bound: 3.0,
            upper_bound: 6.0,
        };
        
        // Average word length: (3 + 5 + 4) / 3 = 4.0
        let data = json!({
            "content": "the sample text",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_process_empty_text() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 0.0,
            upper_bound: 10.0,
        };
        
        let data = json!({
            "text": "",
            "other_field": "value"
        });
        
        // This should panic or return an error since division by zero would occur
        let result = filter.process(data);
        assert!(result.is_err() || result.unwrap().is_none());
    }


    #[test]
    fn test_process_single_word() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 3.0,
            upper_bound: 10.0,
        };
        
        // Average word length: 7.0
        let data = json!({
            "text": "example",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_process_punctuation() {
        let filter = WordLenFilter {
            text_field: String::from("text"),
            lower_bound: 5.0,
            upper_bound: 6.0,
        };
        
        // This uses split_whitespace(), so punctuation is included in word length
        // Average word length: (5 + 5 + 6) / 3 = 5.33
        let data = json!({
            "text": "test, word? hello!",
            "other_field": "value"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }
}