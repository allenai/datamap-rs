extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, WordCountAdder};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_word_count_adder_new_with_defaults() {
        // Test that default values are used when not specified in config
        let config = json!({});
        let processor = WordCountAdder::new(&config).unwrap();
        
        assert_eq!(processor.text_field, "text");
        assert_eq!(processor.word_count_field, "original_word_count");
    }

    #[test]
    fn test_word_count_adder_new_with_custom_config() {
        // Test that custom values are used when specified in config
        let config = json!({
            "text_field": "custom_text",
            "word_count_field": "word_count"
        });
        let processor = WordCountAdder::new(&config).unwrap();
        
        assert_eq!(processor.text_field, "custom_text");
        assert_eq!(processor.word_count_field, "word_count");
    }

    #[test]
    fn test_process_single_word() {
        let processor = WordCountAdder {
            text_field: String::from("text"),
            word_count_field: String::from("word_count")
        };
        
        let input = json!({
            "text": "Hello"
        });
        
        let result = processor.process(input).unwrap().unwrap();
        
        assert_eq!(result["word_count"], 1);
    }

    #[test]
    fn test_process_multiple_words() {
        let processor = WordCountAdder {
            text_field: String::from("text"),
            word_count_field: String::from("word_count")
        };
        
        let input = json!({
            "text": "Hello world, this is a test"
        });
        
        let result = processor.process(input).unwrap().unwrap();
        
        assert_eq!(result["word_count"], 6);
    }

    #[test]
    fn test_process_empty_string() {
        let processor = WordCountAdder {
            text_field: String::from("text"),
            word_count_field: String::from("word_count")
        };
        
        let input = json!({
            "text": ""
        });
        
        let result = processor.process(input).unwrap().unwrap();
        
        assert_eq!(result["word_count"], 0);
    }

    #[test]
    fn test_process_custom_field_names() {
        let processor = WordCountAdder {
            text_field: String::from("custom_text"),
            word_count_field: String::from("custom_count")
        };
        
        let input = json!({
            "custom_text": "This has six words in it"
        });
        
        let result = processor.process(input).unwrap().unwrap();
        assert_eq!(result["custom_count"], 6);
    }

    #[test]
    fn test_process_preserves_other_fields() {
        let processor = WordCountAdder {
            text_field: String::from("text"),
            word_count_field: String::from("word_count")
        };
        
        let input = json!({
            "text": "Hello world",
            "other_field": "value",
            "number": 42
        });
        
        let result = processor.process(input).unwrap().unwrap();
        
        assert_eq!(result["word_count"], 2);
        assert_eq!(result["other_field"], "value");
        assert_eq!(result["number"], 42);
    }

    #[test]
    fn test_process_with_special_characters() {
        let processor = WordCountAdder {
            text_field: String::from("text"),
            word_count_field: String::from("word_count")
        };
        
        let input = json!({
            "text": "Hello, world! This has some special-characters and punctuation."
        });
        
        let result = processor.process(input).unwrap().unwrap();
        
        assert_eq!(result["word_count"], 9);
    }

}