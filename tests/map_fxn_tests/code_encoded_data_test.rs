extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, CodeEncodedData};

#[cfg(test)]
mod code_encoded_data_tests {
    use super::*;
    use serde_json::json;
    use regex::Regex;

    fn create_test_filter() -> CodeEncodedData {
        let config = json!({
            "text_field": "content",
            "single_match_upper_bound_len": 100,
            "total_match_upper_bound_frac": 0.3
        });
        CodeEncodedData::new(&config).unwrap()
    }

    // Custom implementation of process to test regex matching behavior
    fn test_encoded_detection(text: &str) -> Vec<(usize, usize)> {
        let base64_pattern = Regex::new(r"[a-zA-Z0-9+/\n=]{64,}").unwrap();
        let hex_pattern = Regex::new(r"(?:\b(?:0x|\\x)?[0-9a-fA-F]{2}(?:,|\b\s*)){8,}").unwrap();
        let unicode_pattern = Regex::new(r"(?:\\u[0-9a-fA-F]{4}){8,}").unwrap();

        let mut matches = Vec::new();
        for pattern in &[&base64_pattern, &hex_pattern, &unicode_pattern] {
            for mat in pattern.find_iter(text) {
                matches.push((mat.start(), mat.end()));
            }
        }
        matches
    }

    #[test]
    fn test_no_encoded_data_passes() {
        let filter = create_test_filter();
        let data = json!({
            "content": "function main() {\n    console.log('Hello, world!');\n}"
        });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_base64_regex_pattern_matches() {
        // Test the base64 pattern directly
        let base64_pattern = Regex::new(r"[a-zA-Z0-9+/\n=]{64,}").unwrap();
        let base64_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/===========";
        
        // Verify pattern matches
        assert!(base64_pattern.is_match(base64_data));
    }
    
    #[test]
    fn test_hex_regex_pattern_matches() {
        // Test the hex pattern directly
        let hex_pattern = Regex::new(r"(?:\b(?:0x|\\x)?[0-9a-fA-F]{2}(?:,|\b\s*)){8,}").unwrap();
        
        // Test hex pattern with 0x prefix
        let hex_data1 = "0x00 0x11 0x22 0x33 0x44 0x55 0x66 0x77 0x88";
        assert!(hex_pattern.is_match(hex_data1));
        
        // Test hex pattern with \x prefix
        let hex_data2 = "\\x00\\x11\\x22\\x33\\x44\\x55\\x66\\x77\\x88";
        assert!(hex_pattern.is_match(hex_data2));
        
        // Test hex pattern with no prefix
        let hex_data3 = "00 11 22 33 44 55 66 77 88 99";
        assert!(hex_pattern.is_match(hex_data3));
        
        // Test hex pattern with commas
        let hex_data4 = "00,11,22,33,44,55,66,77,88";
        assert!(hex_pattern.is_match(hex_data4));
    }
    
    #[test]
    fn test_unicode_regex_pattern_matches() {
        // Test the unicode pattern directly
        let unicode_pattern = Regex::new(r"(?:\\u[0-9a-fA-F]{4}){8,}").unwrap();
        
        let unicode_data = "\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\u0008";
        assert!(unicode_pattern.is_match(unicode_data));
    }

    #[test]
    fn test_base64_encoded_data_under_single_limit_passes() {
        let config = json!({
            "text_field": "content",
            "single_match_upper_bound_len": 100,
        });    	
        let filter = CodeEncodedData::new(&config).unwrap();

        // Base64-like string under our single match limit of 100
        let base64_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=".repeat(1);
        let content = format!("const token = '{}';", base64_data);
        
        // First verify our regex would actually match this
        let matches = test_encoded_detection(&content);
        // If our regex doesn't match, this test is invalid
        if !matches.is_empty() {
            let data = json!({ "content": content });
            
            // Each match should be under the single limit of 100
            let all_under_limit = matches.iter().all(|(start, end)| (end - start) <= 100);
            
            if all_under_limit {
                assert!(filter.process(data).unwrap().is_some());
            } else {
                assert!(false, "Test is invalid: match exceeds single limit but we expected it to be under limit");
            }
        } else {
            assert!(false, "Test is invalid: base64 pattern not matching when it should");
        }
    }

    #[test]
    fn test_base64_encoded_data_exceeding_single_limit_fails() {
        let filter = create_test_filter();
        // Base64-like string that exceeds our single match limit of 100
        let base64_long = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=".repeat(2);
        let content = format!("const token = '{}';", base64_long);
        
        // First verify our regex would actually match this
        let matches = test_encoded_detection(&content);
        // If our regex doesn't match, this test is invalid
        if !matches.is_empty() {
            let data = json!({ "content": content });
            
            // If any match exceeds the single limit of 100, it should fail
            let any_exceeds_limit = matches.iter().any(|(start, end)| (end - start) > 100);
            
            if any_exceeds_limit {
                assert!(filter.process(data).unwrap().is_none());
            } else {
                assert!(false, "Test is invalid: no match exceeds single limit but we expected at least one to do so");
            }
        } else {
            assert!(false, "Test is invalid: base64 pattern not matching when it should");
        }
    }

    #[test]
    fn test_hex_encoded_data_detection() {
        // Hex pattern that should be detected
        let hex_data = "0x00 0x11 0x22 0x33 0x44 0x55 0x66 0x77 0x88 0x99 0xAA 0xBB 0xCC 0xDD 0xEE 0xFF";
        let content = format!("const hexValue = {};", hex_data);
        
        // Verify our test regex actually matches this
        let matches = test_encoded_detection(&content);
        assert!(!matches.is_empty(), "Hex pattern should be detected");
    }

    #[test]
    fn test_unicode_encoded_data_detection() {
        // Unicode pattern that should be detected
        let unicode_data = "\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\u0008";
        let content = format!("const unicodeValue = '{}';", unicode_data);
        
        // Verify our test regex actually matches this
        let matches = test_encoded_detection(&content);
        assert!(!matches.is_empty(), "Unicode pattern should be detected");
    }

    #[test]
    fn test_total_fraction_calculation() {
        let filter = create_test_filter();
        // Create content with multiple encoded patterns
        let base64_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
        let hex_data = "0x00 0x11 0x22 0x33 0x44 0x55 0x66 0x77 0x88 0x99 0xAA 0xBB";
        let unicode_data = "\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\u0008";
        
        // Create a content where we can control the ratio of encoded to total,
        let filler = "X".repeat(100); // Non-matching content
        let content = format!(
            "{} {} {} {}",
            filler, base64_data, hex_data, unicode_data
        );
        
        // Calculate the total length and encoded length
        let total_len = content.len() as f64;
        let matches = test_encoded_detection(&content);
        let encoded_len = matches.iter().map(|(start, end)| end - start).sum::<usize>() as f64;
        
        // Calculate the ratio
        let ratio = encoded_len / total_len;
        
        let data = json!({ "content": content });
        
        if ratio <= 0.3 {
            // Should pass if under our threshold of 0.3
            assert!(filter.process(data).unwrap().is_some());
        } else {
            // Should fail if over our threshold
            assert!(filter.process(data).unwrap().is_none());
        }
    }

    #[test]
    fn test_multiple_encoded_segments_exceeding_total_limit_fails() {
        let filter = create_test_filter();
        // Create a scenario where the total fraction is exceeded
        
        // Multiple long encoded patterns to ensure we exceed the 0.3 threshold
        let base64_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
        let hex_data = "0x00 0x11 0x22 0x33 0x44 0x55 0x66 0x77 0x88 0x99 0xAA 0xBB 0xCC 0xDD 0xEE 0xFF";
        let unicode_data = "\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\u0008";
        
        // Create content where encoded data will take up more than 30% of total
        // Use small amount of non-encoded text to ensure ratio exceeds threshold
        let filler = "X".repeat(20); // Very small amount of non-matching content
        let content = format!(
            "{} {} {} {} {} {}",
            filler, base64_data, base64_data, hex_data, hex_data, unicode_data
        );
        
        // Calculate the total length and encoded length
        let total_len = content.len() as f64;
        let matches = test_encoded_detection(&content);
        let encoded_len = matches.iter().map(|(start, end)| end - start).sum::<usize>() as f64;
        
        // Calculate the ratio
        let ratio = encoded_len / total_len;
        
        // Only run the assertion if our setup ensures we're exceeding the threshold
        if ratio > 0.3 {
            let data = json!({ "content": content });
            assert!(filter.process(data).unwrap().is_none());
        } else {
            assert!(false, "Test setup invalid: encoded ratio does not exceed threshold");
        }
    }

    #[test]
    fn test_no_limits_specified_defaults_to_max() {
        // Config with no explicit limits
        let config = json!({
            "text_field": "content"
        });
        let filter = CodeEncodedData::new(&config).unwrap();
        
        // Create a very long base64-like string
        let base64_very_long = "A".repeat(1000);
        let content = format!("const token = '{}';", base64_very_long);
        
        // Even a very long match should pass if we default to MAX values
        let data = json!({ "content": content });
        assert!(filter.process(data).unwrap().is_some());
    }

    #[test]
    fn test_overlapping_encoded_patterns() {
        let filter = create_test_filter();
        
        // Create a string that might match multiple patterns
        let overlapping = "0x00\\u0001\\u00020x11";
        let content = format!("const mixed = '{}';", overlapping);
        
        // For this test, we're more interested in ensuring the processing logic handles
        // potentially overlapping matches correctly rather than the specific result
        let data = json!({ "content": content });
        
        // Just verify it doesn't crash and returns a valid result
        let result = filter.process(data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_content_passes() {
        let filter = create_test_filter();
        let data = json!({ "content": "" });
        
        // Empty content has no encoded data, so should pass
        assert!(filter.process(data).unwrap().is_some());
    }
}