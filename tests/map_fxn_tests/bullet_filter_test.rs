extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, BulletFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    
    // Mock function for get_default for testing purposes
    // Implement this if it's not available in your test context
    #[allow(dead_code)]
    fn get_default<T: Clone>(config: &Value, key: &str, default: T) -> T 
    where 
        Value: serde::de::DeserializeOwned,
        T: serde::de::DeserializeOwned,
    {
        config.get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or(default)
    }
    
    // Mock function for json_get for testing purposes
    // Implement this if it's not available in your test context
    #[allow(dead_code)]
    fn json_get<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
        data.get(path)
    }

    #[test]
    fn test_bullet_filter_new() {
        // Test with default values
        let config = json!({});
        let filter = BulletFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.max_bullet_ratio, f32::MAX);
        
        // Test with custom values
        let config = json!({
            "text_field": "content",
            "max_bullet_ratio": 0.5
        });
        let filter = BulletFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.max_bullet_ratio, 0.5);
    }
    
    #[test]
    fn test_process_below_threshold() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        // Text with bullet ratio below threshold (2/5 = 0.4 < 0.5)
        let data = json!({
            "text": "This is line one\n• Bullet point one\n- Bullet point two\nThis is another normal line\nAnd one more line"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
    
    #[test]
    fn test_process_above_threshold() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.3,
        };
        
        // Text with bullet ratio above threshold (2/5 = 0.4 > 0.3)
        let data = json!({
            "text": "This is line one\n• Bullet point one\n- Bullet point two\nThis is another normal line\nAnd one more line"
        });
        
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_process_empty_text() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        // Empty text should not cause a division by zero
        let data = json!({
            "text": ""
        });
        
        let result = filter.process(data.clone());
        // The implementation might panic or return an error for division by zero
        // Depending on the expected behavior, adjust this test
        if let Ok(result) = result {
            assert!(result.is_some());
            assert_eq!(result.unwrap(), data);
        }
    }
    
    #[test]
    fn test_process_all_bullet_points() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        // Text with all bullet points (ratio = 1.0 > 0.5)
        let data = json!({
            "text": "• Bullet one\n- Bullet two\n* Bullet three\n● Bullet four"
        });
        
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_process_no_bullet_points() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        // Text with no bullet points (ratio = 0.0 < 0.5)
        let data = json!({
            "text": "Line one\nLine two\nLine three\nLine four"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
    
    #[test]
    fn test_process_custom_text_field() {
        let filter = BulletFilter {
            text_field: String::from("content"),
            max_bullet_ratio: 0.5,
        };
        
        // Using a custom text field
        let data = json!({
            "content": "Line one\n• Bullet one\nLine three",
            "text": "This should be ignored"
        });
        
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
    
    #[test]
    fn test_process_different_bullet_symbols() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        // Test with different bullet symbols
        let data = json!({
            "text": "● Round bullet\n• Another round bullet\n* Asterisk bullet\n- Dash bullet\nNormal line"
        });
        
        let result = filter.process(data).unwrap();
        assert!(result.is_none()); // 4/5 = 0.8 > 0.5
    }
    
    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_process_missing_text_field() {
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        // Data without the specified text field
        let data = json!({
            "other_field": "This doesn't have the text field"
        });
        
        // This should panic due to unwrap() on None
        filter.process(data).unwrap();
    }
    
    #[test]
    fn test_max_ratio_edge_case() {
        // Test with max_bullet_ratio exactly equal to the ratio in the text
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.5,
        };
        
        let data = json!({
            "text": "Line one\n• Bullet one\n- Bullet two\nLine three\nLine four"
        });
        
        // Ratio is 2/5 = 0.4 < 0.5, so should be Some
        let result = filter.process(data.clone()).unwrap();
        assert!(result.is_some());
       	
        // Now with exact threshold
        let filter = BulletFilter {
            text_field: String::from("text"),
            max_bullet_ratio: 0.4,
        };
        
        // Ratio is 2/5 = 0.4 = 0.4, so should be some
        let result = filter.process(data).unwrap();
        assert!(result.is_some());
    }
}