extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, UrlSubstringFilter};


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::collections::HashSet;
    

    // Helper function to create a basic config for testing
    fn create_test_config(
        url_key: &str,
        ignore_chars: Vec<&str>,
        num_banned_substrs: usize,
        exact_domain_match: bool,
        match_substrings: bool,
        case_sensitive: bool,
    ) -> Value {
        json!({
            "url_key": url_key,
            "ignore_chars": ignore_chars,
            "num_banned_substrs": num_banned_substrs,
            "exact_domain_match": exact_domain_match,
            "match_substrings": match_substrings,
            "case_sensitive": case_sensitive,
        })
    }

    // Test constructor with explicit banlist
    #[test]
    fn test_construct_w_explicit_banlist() {
        let config = create_test_config("url", vec!["-", "."], 1, false, true, false);
        let banlist: HashSet<String> = vec!["bad", "evil", "malicious"].into_iter().map(String::from).collect();
        
        let result = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist.clone());
        
        assert!(result.is_ok());
        let filter = result.unwrap();
        
        assert_eq!(filter.url_key, "url");
        assert_eq!(filter.ignore_chars, vec!["-", "."].into_iter().map(String::from).collect::<Vec<String>>());
        assert_eq!(filter.num_banned_substrs, 1);
        assert_eq!(filter.exact_domain_match, false);
        assert_eq!(filter.match_substrings, true);
        assert_eq!(filter.case_sensitive, false);
        assert_eq!(filter.banlist, banlist);
        assert!(filter.ac_banlist.is_some()); // Since exact_domain_match is false
    }

    #[test]
    fn test_construct_w_explicit_banlist_exact_match() {
        let config = create_test_config("url", vec![], 1, true, false, true);
        let banlist: HashSet<String> = vec!["example.com", "bad.com"].into_iter().map(String::from).collect();
        
        let result = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist.clone());
        
        assert!(result.is_ok());
        let filter = result.unwrap();
        
        assert_eq!(filter.exact_domain_match, true);
        assert_eq!(filter.case_sensitive, true);
        assert!(filter.ac_banlist.is_none()); // Since exact_domain_match is true
    }

    // Test process method with exact domain matching
    #[test]
    fn test_process_exact_domain_match() {
        let config = create_test_config("url", vec![], 1, true, false, false);
        let banlist: HashSet<String> = vec!["example.com"].into_iter().map(String::from).collect();
        
        let filter = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist).unwrap();
        
        // Test with banned URL
        let data = json!({"url": "https://example.com/page"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should be filtered out
        
        // Test with non-banned URL
        let data = json!({"url": "https://safe.com/page"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Should pass through
    }

    // Test process method with substring matching
    #[test]
    fn test_process_substring_match() {
        let config = create_test_config("url", vec![], 1, false, true, false);
        let banlist: HashSet<String> = vec!["bad", "evil"].into_iter().map(String::from).collect();
        
        let filter = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist).unwrap();
        
        // Test with URL containing banned substring
        let data = json!({"url": "https://containsbad.com"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should be filtered out
        
        // Test with safe URL
        let data = json!({"url": "https://safe.com"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Should pass through
    }

    // Test process method with character ignoring
    #[test]
    fn test_process_ignore_chars() {
        let config = create_test_config("url", vec!["."], 1, false, true, false);
        let banlist: HashSet<String> = vec!["badcom"].into_iter().map(String::from).collect();
        
        let filter = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist).unwrap();
        
        // Test with URL containing dots that should be ignored
        let data = json!({"url": "https://bad.com/page"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should be filtered out after dots are removed
        
        // Test with safe URL
        let data = json!({"url": "https://safe.org"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Should pass through
    }

    // Test process method with case sensitivity
    #[test]
    fn test_process_case_sensitivity() {
        // Case sensitive
        let config = create_test_config("url", vec![], 1, false, true, true);
        let banlist: HashSet<String> = vec!["Bad.com"].into_iter().map(String::from).collect();
        
        let filter = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist).unwrap();
        
        // Test with URL containing banned substring but different case
        let data = json!({"url": "bad.com"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Should pass through due to case mismatch
        
        // Case insensitive
        let config = create_test_config("url", vec![], 1, false, true, false);
        let banlist: HashSet<String> = vec!["bad.com"].into_iter().map(String::from).collect();
        
        let filter = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist).unwrap();
        
        // Test with URL containing banned substring with different case
        let data = json!({"url": "Bad.com"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should be filtered out as case insensitive
    }

    // Test process method with num_banned_substrs
    #[test]
    fn test_process_num_banned_substrs() {
        let config = create_test_config("url", vec![], 2, false, true, false);
        let banlist: HashSet<String> = vec!["bad", "evil"].into_iter().map(String::from).collect();
        
        let filter = UrlSubstringFilter::construct_w_explicit_banlist(&config, banlist).unwrap();
        
        // Test with URL containing just one banned substring
        let data = json!({"url": "https://badsite.com"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_some()); // Should pass through (requires 2 matches)
        
        // Test with URL containing two banned substrings
        let data = json!({"url": "https://badevilsite.com"});
        let result = filter.process(data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Should be filtered out (has 2 matches)
    }



}