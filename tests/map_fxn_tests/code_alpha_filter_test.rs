extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, CodeAlphaFilter};


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json};
   

    fn create_test_filter() -> CodeAlphaFilter {
        let config = json!({
            "text_field": "content",
            "alpha_lower_bound": 0.5,
            "exclude_field": "language",
            "exclude_vals": ["svg", "json"]
        });
        CodeAlphaFilter::new(&config).unwrap()
    }

    #[test]
    fn test_high_alpha_content_passes() {
        let filter = create_test_filter();
        let data = json!({
            "content": "function calculateSum(a, b) { return a + b; }",
            "language": "javascript"
        });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_low_alpha_content_fails() {
        let filter = create_test_filter();
        let data = json!({
            "content": "/*****\n*******\n++++++\n-----\n>>>>>\n<<<<<\n*****",
            "language": "javascript"
        });
        assert!(filter.process(data).unwrap().is_none());
    }

    #[test]
    fn test_excluded_language_bypasses_filter() {
        let filter = create_test_filter();
        let data = json!({
            "content": "/*****\n*******\n++++++\n-----\n>>>>>\n<<<<<\n*****",
            "language": "svg"
        });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_another_excluded_language_bypasses_filter() {
        let filter = create_test_filter();
        let data = json!({
            "content": "/*****\n*******\n++++++\n-----\n>>>>>\n<<<<<\n*****",
            "language": "json"
        });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_exactly_at_threshold_fails() {
        let filter = create_test_filter();
        // 3/6 = 0.5 alphanumeric ratio, which is exactly the threshold
        let data = json!({
            "content": "abc!@#",
            "language": "javascript"
        });
        // This should actually pass at exactly the threshold, but checking the code:
        // `if alpha_len < total_len * self.alpha_lower_bound {`
        // It will fail if strictly less than the threshold
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_below_threshold_fails() {
        let filter = create_test_filter();
        // 2/6 = 0.33 alphanumeric ratio, which is below the threshold
        let data = json!({
            "content": "ab!@#$",
            "language": "javascript"
        });
        assert!(filter.process(data).unwrap().is_none());
    }

    #[test]
    fn test_above_threshold_passes() {
        let filter = create_test_filter();
        // 4/6 = 0.66 alphanumeric ratio, which is above the threshold
        let data = json!({
            "content": "abcd!@",
            "language": "javascript"
        });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_different_threshold_config() {
        let config = json!({
            "text_field": "content",
            "alpha_lower_bound": 0.3,
            "exclude_field": "language",
            "exclude_vals": ["svg", "json"]
        });
        let filter = CodeAlphaFilter::new(&config).unwrap();
        
        // 2/6 = 0.33 alphanumeric ratio, which is above the new threshold
        let data = json!({
            "content": "ab!@#$",
            "language": "javascript"
        });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_no_exclude_field_config() {
        let config = json!({
            "text_field": "content",
            "alpha_lower_bound": 0.5
        });
        let filter = CodeAlphaFilter::new(&config).unwrap();
        
        // Should apply the filter since no exclusions are defined
        let data = json!({
            "content": "/*****\n*******\n++++++\n-----\n>>>>>\n<<<<<\n*****",
            "language": "svg"
        });
        assert!(filter.process(data).unwrap().is_none());
    }
}