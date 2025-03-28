extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, WordRemovalRatioFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_word_removal_ratio_filter_new() {
        // Test with default values
        let config = json!({});
        let filter = WordRemovalRatioFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.word_count_field, "original_word_count");
        assert_eq!(filter.upper_bound, 1.0);

        // Test with custom values
        let config = json!({
            "text_field": "content",
            "word_count_field": "prev_word_count",
            "upper_bound": 0.5
        });
        let filter = WordRemovalRatioFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.word_count_field, "prev_word_count");
        assert_eq!(filter.upper_bound, 0.5);
    }

    #[test]
    fn test_process_within_bound() {
        let filter = WordRemovalRatioFilter {
            text_field: String::from("text"),
            word_count_field: String::from("original_word_count"),
            upper_bound: 0.3,
        };

        // Original has 10 words, current has 8 words (20% removal, within 30% bound)
        let data = json!({
            "text": "this is a sample text with eight words",
            "original_word_count": 10,
            "other_field": "value"
        });

        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_process_at_bound() {
        let filter = WordRemovalRatioFilter {
            text_field: String::from("text"),
            word_count_field: String::from("original_word_count"),
            upper_bound: 0.3,
        };

        // Original has 10 words, current has 7 words (30% removal, exactly at bound)
        let data = json!({
            "text": "this is sample text with seven words",
            "original_word_count": 10,
            "other_field": "value"
        });

        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_process_exceeds_bound() {
        let filter = WordRemovalRatioFilter {
            text_field: String::from("text"),
            word_count_field: String::from("original_word_count"),
            upper_bound: 0.3,
        };

        // Original has 10 words, current has 6 words (40% removal, exceeds 30% bound)
        let data = json!({
            "text": "this sample text has six words",
            "original_word_count": 10,
            "other_field": "value"
        });

        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_process_no_removal() {
        let filter = WordRemovalRatioFilter {
            text_field: String::from("text"),
            word_count_field: String::from("original_word_count"),
            upper_bound: 0.3,
        };

        // Original has 10 words, current has 10 words (0% removal, within bound)
        let data = json!({
            "text": "this is a sample text with exactly ten words here",
            "original_word_count": 10,
            "other_field": "value"
        });

        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_process_custom_field_names() {
        let filter = WordRemovalRatioFilter {
            text_field: String::from("content"),
            word_count_field: String::from("prev_count"),
            upper_bound: 0.25,
        };

        // Original has 8 words, current has 6 words (25% removal, at bound)
        let data = json!({
            "content": "custom field names should work too",
            "prev_count": 8,
            "other_field": "value"
        });

        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_process_with_unicode() {
        let filter = WordRemovalRatioFilter {
            text_field: String::from("text"),
            word_count_field: String::from("original_word_count"),
            upper_bound: 0.5,
        };

        // Original has 8 words, current has 7 words with unicode (12.5% removal, within bound)
        let data = json!({
            "text": "text with unicode 你好 世界",
            "original_word_count": 8,
            "other_field": "value"
        });

        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

}