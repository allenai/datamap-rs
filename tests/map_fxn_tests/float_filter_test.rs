extern crate datamap_rs; 
use datamap_rs::map_fxn::{DataProcessor, FloatFilter};

mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_float_filter_new() {
        // Test with all fields specified
        let config = json!({
            "float_field": "temperature",
            "lower_bound": 20.5,
            "upper_bound": 30.75,
            "default": 25.0
        });
        
        let filter = FloatFilter::new(&config).unwrap();
        assert_eq!(filter.float_field, "temperature");
        assert_eq!(filter.lower_bound, 20.5);
        assert_eq!(filter.upper_bound, 30.75);
        assert_eq!(filter.default, 25.0);
        
        // Test with minimal fields (using defaults)
        let config = json!({
            "float_field": "weight"
        });
        
        let filter = FloatFilter::new(&config).unwrap();
        assert_eq!(filter.float_field, "weight");
        assert_eq!(filter.lower_bound, 0.0);
        assert_eq!(filter.upper_bound, f32::MAX);
        assert_eq!(filter.default, 0.0);
    }

    #[test]
    fn test_process_value_in_range() {
        let filter = FloatFilter {
            float_field: "temperature".to_string(),
            lower_bound: 20.0,
            upper_bound: 30.0,
            default: 0.0,
        };
        
        // Test value in range (should return the document)
        let doc = json!({
            "id": "sensor1",
            "temperature": 25.5
        });
        
        let result = filter.process(doc.clone()).unwrap();
        assert_eq!(result, Some(doc));
    }
    
    #[test]
    fn test_process_value_at_boundaries() {
        let filter = FloatFilter {
            float_field: "temperature".to_string(),
            lower_bound: 20.0,
            upper_bound: 30.0,
            default: 0.0,
        };
        
        // Test value at lower bound (should be included)
        let doc_lower = json!({
            "id": "sensor1",
            "temperature": 20.0
        });
        
        let result = filter.process(doc_lower.clone()).unwrap();
        assert_eq!(result, Some(doc_lower));
        
        // Test value at upper bound (should be included)
        let doc_upper = json!({
            "id": "sensor2",
            "temperature": 30.0
        });
        
        let result = filter.process(doc_upper.clone()).unwrap();
        assert_eq!(result, Some(doc_upper));
    }
    
    #[test]
    fn test_process_value_out_of_range() {
        let filter = FloatFilter {
            float_field: "temperature".to_string(),
            lower_bound: 20.0,
            upper_bound: 30.0,
            default: 0.0,
        };
        
        // Test value below range
        let doc_below = json!({
            "id": "sensor1",
            "temperature": 15.5
        });
        
        let result = filter.process(doc_below).unwrap();
        assert_eq!(result, None);
        
        // Test value above range
        let doc_above = json!({
            "id": "sensor2",
            "temperature": 35.0
        });
        
        let result = filter.process(doc_above).unwrap();
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_process_missing_field() {
        let filter = FloatFilter {
            float_field: "temperature".to_string(),
            lower_bound: 20.0,
            upper_bound: 30.0,
            default: 25.0,
        };
        
        // Test missing field (should use default value)
        let doc = json!({
            "id": "sensor1"
            // temperature field is missing
        });
        
        let result = filter.process(doc.clone()).unwrap();
        // Since default is 25.0 which is in range, document should pass
        assert_eq!(result, Some(doc.clone()));
        
        // Test with default value out of range
        let filter_with_out_of_range_default = FloatFilter {
            float_field: "temperature".to_string(),
            lower_bound: 20.0,
            upper_bound: 30.0,
            default: 10.0, // Out of range
        };
        
        let result = filter_with_out_of_range_default.process(doc).unwrap();
        assert_eq!(result, None);
    }
    
    #[test]
    fn test_process_nested_field() {
        let filter = FloatFilter {
            float_field: "readings.temperature".to_string(),
            lower_bound: 20.0,
            upper_bound: 30.0,
            default: 0.0,
        };
        
        // Test nested field in range
        let doc = json!({
            "id": "sensor1",
            "readings": {
                "temperature": 25.5,
                "humidity": 40.0
            }
        });
        
        let result = filter.process(doc.clone()).unwrap();
        assert_eq!(result, Some(doc));
        
        // Test nested field out of range
        let doc_out = json!({
            "id": "sensor2",
            "readings": {
                "temperature": 15.0,
                "humidity": 50.0
            }
        });
        
        let result = filter.process(doc_out).unwrap();
        assert_eq!(result, None);
    }
}
