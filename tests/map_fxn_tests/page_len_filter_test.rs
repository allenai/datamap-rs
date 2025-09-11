extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, PageLenFilter, LengthType};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // Helper function to create a test document
    fn create_test_doc(text: &str) -> Value {
        json!({ "text": text })
    }

    #[test]
    fn test_pagelength_filter_creation() {
        // Test with minimal config
        let config = json!({
            "length_type": "word",
        });
        let filter = PageLenFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.length_type, "word".parse::<LengthType>().unwrap());
        assert_eq!(filter.lower_bound, 1);
        assert_eq!(filter.upper_bound, usize::MAX);
        assert_eq!(filter.ignore_punctuation, true);

        // Test with full config
        let config = json!({
            "text_field": "content",
            "length_type": "word",
            "lower_bound": 5,
            "upper_bound": 10,
            "ignore_punctuation": false
        });
        let filter = PageLenFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.length_type, "word".parse::<LengthType>().unwrap());
        assert_eq!(filter.lower_bound, 5);
        assert_eq!(filter.upper_bound, 10);
        assert_eq!(filter.ignore_punctuation, false);
    }

    #[test]
    fn test_invalid_length_type() {
        let config = json!({
            "length_type": "invalid",
        });
        let result = PageLenFilter::new(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_word_length_within_bounds() {
        let config = json!({
            "length_type": "word",
            "lower_bound": 3,
            "upper_bound": 5
        });
        let filter = PageLenFilter::new(&config).unwrap();
        
        // Test exactly 3 words
        let doc = create_test_doc("one two three");
        assert!(filter.process(doc).unwrap().is_some());
        
        // Test exactly 5 words
        let doc = create_test_doc("one two three four five");
        assert!(filter.process(doc).unwrap().is_some());
        
        // Test 4 words (in bounds)
        let doc = create_test_doc("one two three four");
        assert!(filter.process(doc).unwrap().is_some());
    }

    #[test]
    fn test_word_length_outside_bounds() {
        let config = json!({
            "length_type": "word",
            "lower_bound": 3,
            "upper_bound": 5
        });
        let filter = PageLenFilter::new(&config).unwrap();
        
        // Test 2 words (below lower bound)
        let doc = create_test_doc("one two");
        assert!(filter.process(doc).unwrap().is_none());
        
        // Test 6 words (above upper bound)
        let doc = create_test_doc("one two three four five six");
        assert!(filter.process(doc).unwrap().is_none());
    }

    #[test]
    fn test_punctuation_handling() {
        // Test with ignore_punctuation = true (default)
        let config = json!({
            "length_type": "word",
            "lower_bound": 3,
            "upper_bound": 3
        });
        let filter = PageLenFilter::new(&config).unwrap();
        
        // This should count as 3 words with punctuation ignored
        let doc = create_test_doc("one, two. three!");
        assert!(filter.process(doc).unwrap().is_some());
        
        // Test with ignore_punctuation = false
        let config = json!({
            "length_type": "word",
            "lower_bound": 6,
            "upper_bound": 6,
            "ignore_punctuation": false
        });
        let filter = PageLenFilter::new(&config).unwrap();
        
        // This should count as 6 words including punctuation
        let doc = create_test_doc("one, two. three!");
        assert!(filter.process(doc).unwrap().is_some());
    }

    #[test]
    fn test_custom_text_field() {
        let config = json!({
            "text_field": "content",
            "length_type": "word",
            "lower_bound": 4,
            "upper_bound": 4
        });
        let filter = PageLenFilter::new(&config).unwrap();
        
        // Test with custom text field
        let doc = json!({ "content": "one two three four" });
        assert!(filter.process(doc).unwrap().is_some());
    }

    #[test]
    fn test_unimplemented_length_types() {
        // Test other length types which are not yet implemented
        for length_type in ["unimplemented"].iter() {
            let config = json!({
                "length_type": length_type,
            });
            let filter = PageLenFilter::new(&config); 
            assert!(filter.is_err());
        }
    }

    #[test]
    fn test_edge_cases() {
        let config = json!({
            "length_type": "word",
            "lower_bound": 1,
            "upper_bound": 10
        });
        let filter = PageLenFilter::new(&config).unwrap();
        
        // Empty text
        let doc = create_test_doc("");
        assert!(filter.process(doc).unwrap().is_none());
        
        // Text with only punctuation
        let config = json!({
            "length_type": "word",
            "lower_bound": 0,
            "upper_bound": 10,
            "ignore_punctuation": true
        });
        let filter = PageLenFilter::new(&config).unwrap();
        let doc = create_test_doc(".,;!?");
        assert!(filter.process(doc).unwrap().is_some());
        
    }
}