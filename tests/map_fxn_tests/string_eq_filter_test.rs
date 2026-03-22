extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, StringEqFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_process_matches_string_field() {
        let filter = StringEqFilter {
            str_field: "kind".to_string(),
            eq: "commit".to_string(),
            keep_matches: true,
        };

        let doc = json!({ "kind": "commit" });
        let result = filter.process(doc.clone()).unwrap();
        assert_eq!(result, Some(doc));
    }

    #[test]
    fn test_process_treats_non_string_field_as_empty_string() {
        let filter = StringEqFilter {
            str_field: "kind".to_string(),
            eq: "".to_string(),
            keep_matches: true,
        };

        let doc = json!({ "kind": 42 });
        let result = filter.process(doc.clone()).unwrap();
        assert_eq!(result, Some(doc));
    }

    #[test]
    fn test_process_treats_missing_field_as_empty_string() {
        let filter = StringEqFilter {
            str_field: "kind".to_string(),
            eq: "".to_string(),
            keep_matches: true,
        };

        let doc = json!({ "other": "value" });
        let result = filter.process(doc.clone()).unwrap();
        assert_eq!(result, Some(doc));
    }
}
