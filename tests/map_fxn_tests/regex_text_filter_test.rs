extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, RegexTextFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_regex_text_filter_new() {
        // Test with default values
        let config = json!({});
        let filter = RegexTextFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.regex_string, "");
        assert!(filter.remove_matches);

        // Test with custom values
        let config = json!({
            "text_field": "content",
            "regex_string": r"^\d+$",
            "remove_matches": false
        });
        let filter = RegexTextFilter::new(&config).unwrap();
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.regex_string, r"^\d+$");
        assert!(!filter.remove_matches);
    }

    #[test]
    fn test_remove_matches_true() {
        // Default behavior: remove documents where text matches regex
        let config = json!({
            "regex_string": r"spam|unwanted"
        });
        let filter = RegexTextFilter::new(&config).unwrap();

        // Should filter out (return None) when regex matches
        let data = json!({ "text": "this is spam content" });
        let result = filter.process(data).unwrap();
        assert!(result.is_none());

        // Should keep (return Some) when regex doesn't match
        let data = json!({ "text": "this is good content" });
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }

    #[test]
    fn test_remove_matches_false() {
        // Keep only documents where text matches regex
        let config = json!({
            "regex_string": r"important|urgent",
            "remove_matches": false
        });
        let filter = RegexTextFilter::new(&config).unwrap();

        // Should keep when regex matches
        let data = json!({ "text": "this is important" });
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));

        // Should filter out when regex doesn't match
        let data = json!({ "text": "this is normal" });
        let result = filter.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_custom_text_field() {
        let config = json!({
            "text_field": "body",
            "regex_string": r"test"
        });
        let filter = RegexTextFilter::new(&config).unwrap();

        let data = json!({ "body": "this is a test" });
        let result = filter.process(data).unwrap();
        assert!(result.is_none());

        let data = json!({ "body": "no match here" });
        let result = filter.process(data.clone()).unwrap();
        assert_eq!(result, Some(data));
    }
}
