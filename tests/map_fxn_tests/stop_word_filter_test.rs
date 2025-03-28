extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, StopWordFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Helper function to create a default StopWordFilter
    fn create_default_filter() -> StopWordFilter {
        StopWordFilter::new(&json!({})).unwrap()
    }

    // Helper function to create a StopWordFilter with custom configuration
    fn create_custom_filter(text_field: &str, count_unique: bool, min_stop_word: usize) -> StopWordFilter {
        let config = json!({
            "text_field": text_field,
            "count_unique": count_unique,
            "min_stop_word": min_stop_word
        });
        StopWordFilter::new(&config).unwrap()
    }

    #[test]
    fn test_new_default_values() {
        let filter = create_default_filter();
        
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.count_unique, false);
        assert_eq!(filter.min_stop_word, 2);
        assert!(filter.stop_words.contains("the"));
        assert_eq!(filter.stop_words.len(), 8); // Check all stop words were added
    }

    #[test]
    fn test_new_custom_values() {
        let filter = create_custom_filter("content", true, 3);
        
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.count_unique, true);
        assert_eq!(filter.min_stop_word, 3);
    }

    #[test]
    fn test_process_count_total_pass() {
        let filter = create_custom_filter("text", false, 2);
        let data = json!({"text": "This is the document with the important content"});
        
        // Should pass because "the" appears twice (non-unique count)
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_process_count_total_fail() {
        let filter = create_custom_filter("text", false, 4);
        let data = json!({"text": "This is the document with the important content"});
        
        // Should fail because "the" appears only twice but we need 4
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_process_count_unique_pass() {
        let filter = create_custom_filter("text", true, 2);
        let data = json!({"text": "The document and the content that have important information"});
        
        // Should pass because there are 3 unique stop words ("the", "and", "that")
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_process_count_unique_fail() {
        let filter = create_custom_filter("text", true, 3);
        let data = json!({"text": "The document with the content"});
        
        // Should fail because there's only 1 unique stop word ("the")
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_case_insensitivity() {
        let filter = create_custom_filter("text", false, 2);
        let data = json!({"text": "This is THE document with The important content"});
        
        // Should pass because "THE" and "The" should be counted as "the"
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_empty_text() {
        let filter = create_default_filter();
        let data = json!({"text": ""});
        
        // Should fail because there are no stop words
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_no_stop_words() {
        let filter = create_default_filter();
        let data = json!({"text": "This is a document without any stop words from list"});
        
        // Should fail because there are no stop words from our list
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_custom_text_field() {
        let filter = create_custom_filter("content", false, 2);
        let data = json!({
            "text": "This field is ignored",
            "content": "This is the document with the important information"
        });
        
        // Should pass because the "content" field has two occurrences of "the"
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_min_stop_word_zero() {
        let filter = create_custom_filter("text", false, 0);
        let data = json!({"text": "This has no stop words at all"});
        
        // Should pass because min_stop_word is 0
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    #[should_panic]
    fn test_missing_text_field() {
        let filter = create_default_filter();
        let data = json!({"other_field": "This will cause an error"});
        
        // Should panic because the text field is missing
        filter.process(data).unwrap();
    }

    #[test]
    fn test_multiple_stop_words() {
        let filter = create_custom_filter("text", false, 3);
        let data = json!({"text": "The document and the content that have important information with details"});
        
        // Should pass because there are 5 stop words total ("the" x2, "and", "that", "with")
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
}