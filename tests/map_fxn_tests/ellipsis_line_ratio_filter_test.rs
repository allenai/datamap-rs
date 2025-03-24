extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, EllipsisLineRatioFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_new_with_defaults() {
        // Test constructor with empty config (should use defaults)
        let config = json!({});
        let filter = EllipsisLineRatioFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.max_ratio, f32::MAX);
    }
    
    #[test]
    fn test_new_with_custom_values() {
        // Test constructor with custom config
        let config = json!({
            "text_field": "content",
            "max_ratio": 0.5
        });
        let filter = EllipsisLineRatioFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.max_ratio, 0.5);
    }
    
    #[test]
    fn test_process_with_no_ellipses() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.3,
        };
        
        let data = json!({
            "text": "Line one\nLine two\nLine three\nLine four"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }
    
    #[test]
    fn test_process_with_acceptable_ellipsis_ratio() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        let data = json!({
            "text": "Line one...\nLine two\nLine three\nLine four"
        });
        
        // 1/4 = 0.25 which is <= 0.5
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }
    
    #[test]
    fn test_process_with_unacceptable_ellipsis_ratio() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.3,
        };
        
        let data = json!({
            "text": "Line one...\nLine two...\nLine three\nLine four"
        });
        
        // 2/4 = 0.5 which is > 0.3
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_different_ellipsis_formats() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        let data = json!({
            "text": "Standard ellipsis...\nSpaced ellipsis. . .\nUnicode ellipsis…\nNo ellipsis"
        });
        
        // 3/4 = 0.75 which is > 0.5
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_empty_lines_are_ignored() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.25,
        };
        
        let data = json!({
            "text": "Line one...\n\nLine two\n\nLine three\nLine four"
        });
        
        // Empty lines are filtered out, so 1/4 = 0.25 which is <= 0.25
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }
    
    #[test]
    fn test_empty_text() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        // Empty strings should be handled gracefully
        let data = json!({
            "text": ""
        });
        
        // No lines, so ratio calculation should handle this special case
        let result = filter.process(data.clone()).unwrap();
        // The implementation would need to handle division by zero here
        // This assertion assumes the current implementation would return Some(data)
        // when there are no lines to process
        assert_eq!(result, Some(data));
    }
    
    #[test]
    fn test_custom_text_field() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("content"),
            max_ratio: 0.3,
        };
        
        let data = json!({
            "content": "Line one\nLine two..."
        });
        
        // 1/2 = 0.5 which is > 0.3
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, None);
    }
    
    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_missing_text_field() {
        let filter = EllipsisLineRatioFilter {
            text_field: String::from("text"),
            max_ratio: 0.5,
        };
        
        let data = json!({
            "content": "Line one\nLine two"  // "text" field is missing
        });
        
        // Should panic because the text field is missing
        let _ = filter.process(data).unwrap();
    }
}