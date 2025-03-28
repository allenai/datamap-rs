extern crate datamap_rs;

use datamap_rs::map_fxn::{DataProcessor, SymbolRatioFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // Helper function to create a document with the given text
    fn create_doc(text: &str) -> Value {
        json!({ "text": text })
    }

    #[test]
    fn test_new_with_defaults() {
        // Test creating with empty config (should use defaults)
        let config = json!({});
        let filter = SymbolRatioFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "text");
        assert_eq!(filter.max_symbol_to_word_ratio, f32::MAX);
    }

    #[test]
    fn test_new_with_custom_config() {
        // Test creating with custom config
        let config = json!({
            "text_field": "content",
            "max_symbol_to_word_ratio": 0.5
        });
        
        let filter = SymbolRatioFilter::new(&config).unwrap();
        
        assert_eq!(filter.text_field, "content");
        assert_eq!(filter.max_symbol_to_word_ratio, 0.5);
    }

    #[test]
    fn test_process_no_symbols() {
        let filter = SymbolRatioFilter::new(&json!({"max_symbol_to_word_ratio": 0.2})).unwrap();
        let doc = create_doc("This is a normal text with no special symbols.");
        
        let result = filter.process(doc).unwrap();
        
        assert!(result.is_some(), "Document should pass the filter");
    }

    #[test]
    fn test_process_with_hashtags() {
        let filter = SymbolRatioFilter::new(&json!({"max_symbol_to_word_ratio": 0.2})).unwrap();
        
        // 2 hashtags, 10 words => ratio = 0.2 (at threshold)
        let doc1 = create_doc("This #post has #hashtags but should still pass the filter.");
        let result1 = filter.process(doc1).unwrap();
        assert!(result1.is_some(), "Document at threshold should pass");
        
        // 3 hashtags, 10 words => ratio = 0.3 (exceeds threshold)
        let doc2 = create_doc("This #post has #too #many hashtags for the filter.");
        let result2 = filter.process(doc2).unwrap();
        assert!(result2.is_none(), "Document exceeding threshold should be filtered out");
    }

    #[test]
    fn test_process_with_ellipsis() {
        let filter = SymbolRatioFilter::new(&json!({"max_symbol_to_word_ratio": 0.25})).unwrap();
        
        // Test with ASCII ellipsis (...)
        let doc1 = create_doc("This text... has one ellipsis... and should pass.");
        // 2 ellipses, 8 words => ratio = 0.25
        let result1 = filter.process(doc1).unwrap();
        assert!(result1.is_some(), "Document with ASCII ellipsis at threshold should pass");
        
        // Test with spaced ellipsis (. . .)
        let doc2 = create_doc("Too many . . . of these . . . ellipses . . . in this text.");
        // 3 ellipses, 9 words => ratio ≈ 0.33 (exceeds threshold)
        let result2 = filter.process(doc2).unwrap();
        assert!(result2.is_none(), "Document with spaced ellipsis exceeding threshold should be filtered");
        
        // Test with Unicode ellipsis (…)
        let doc3 = create_doc("This has a Unicode ellipsis… only one… so it passes.");
        // 2 ellipses, 9 words => ratio ≈ 0.22 (below threshold)
        let result3 = filter.process(doc3).unwrap();
        assert!(result3.is_some(), "Document with Unicode ellipsis below threshold should pass");
    }

    #[test]
    fn test_process_mixed_symbols() {
        let filter = SymbolRatioFilter::new(&json!({"max_symbol_to_word_ratio": 0.3})).unwrap();
        
        // Mix of hashtags and ellipses
        let doc = create_doc("This #text has... mixed #symbols and… should be filtered.");
        // 4 symbols, 10 words => ratio = 0.4 (exceeds threshold)
        let result = filter.process(doc).unwrap();
        assert!(result.is_none(), "Document with mixed symbols exceeding threshold should be filtered");
    }

    #[test]
    fn test_process_empty_text() {
        let filter = SymbolRatioFilter::new(&json!({})).unwrap();
        let doc = create_doc("");
        
        // This should handle the edge case of empty text without panicking
        // The calculation will attempt division by zero, so we should ensure it's handled
        let result = filter.process(doc);
        
        assert!(result.is_err() || result.unwrap().is_some(), 
               "Empty text should either return an error or pass the filter");
    }

    #[test]
    fn test_process_custom_field() {
        let filter = SymbolRatioFilter::new(&json!({
            "text_field": "body",
            "max_symbol_to_word_ratio": 0.2
        })).unwrap();
        
        let doc = json!({
            "body": "This #post has #hashtags and should be filtered.",
            "text": "This field should be ignored."
        });
        
        // 2 hashtags, 8 words => ratio = 0.25 (exceeds threshold)
        let result = filter.process(doc).unwrap();
        assert!(result.is_none(), "Document should be filtered based on 'body' field");
    }
}