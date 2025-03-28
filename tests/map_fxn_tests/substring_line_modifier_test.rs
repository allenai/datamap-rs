extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, SubstringLineModifier};
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn create_test_data(text: &str) -> Value {
        json!({
            "text": text,
            "other_field": "This should remain untouched"
        })
    }

    #[test]
    fn test_new_with_default_values() {
        let config = json!({
            "banlist": "bad"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        
        assert_eq!(processor.text_field, "text");
        assert_eq!(processor.banlist, "bad");
        assert_eq!(processor.max_len, usize::MAX);
        assert_eq!(processor.remove_substring_only, true);
        assert_eq!(processor.location, "any");
    }

    #[test]
    fn test_new_with_custom_values() {
        let config = json!({
            "text_field": "content",
            "banlist": "forbidden",
            "max_len": 10,
            "remove_substring_only": false,
            "location": "prefix"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        
        assert_eq!(processor.text_field, "content");
        assert_eq!(processor.banlist, "forbidden");
        assert_eq!(processor.max_len, 10);
        assert_eq!(processor.remove_substring_only, false);
        assert_eq!(processor.location, "prefix");
    }

    #[test]
    fn test_process_remove_substring() {
        let config = json!({
            "banlist": "bad",
            "remove_substring_only": true
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("This is a bad line.\nThis is a good line.\nThis is a badger line.");
        let result = processor.process(input).unwrap().unwrap();
        
        // "bad" is removed from "bad" and "badger"
        let expected = "This is a line.\nThis is a good line.\nThis is a ger line.";
        assert_eq!(result["text"], expected);
        assert_eq!(result["other_field"], "This should remain untouched");
    }

    #[test]
    fn test_process_remove_line() {
        let config = json!({
            "banlist": "bad",
            "remove_substring_only": false
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("This is a bad line.\nThis is a good line.\nThis is a badger line.");
        let result = processor.process(input).unwrap().unwrap();
        
        // Lines with "bad" anywhere are removed completely
        let expected = "This is a good line.";
        assert_eq!(result["text"], expected);
        assert_eq!(result["other_field"], "This should remain untouched");
    }

    #[test]
    fn test_process_max_len() {
        let config = json!({
            "banlist": "bad",
            "remove_substring_only": false,
            "max_len": 4
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("This is bad line.\nShort line.\nThis is a good long line.");
        let result = processor.process(input).unwrap().unwrap();
        
        // "This is a bad line." has banned word but would be removed anyway due to length
        // "Short line." has 2 words, under max_len, and no banned word - should pass
        // "This is a good long line." has 6 words, over max_len - should pass even without banned words
        let expected = "Short line.\nThis is a good long line.";
        assert_eq!(result["text"], expected);
    }

    #[test]
    fn test_process_prefix_location() {
        let config = json!({
            "banlist": "bad",
            "location": "prefix"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("bad start of line.\nMiddle bad word.\nbadger beginning.\nNormal line.");
        let result = processor.process(input).unwrap().unwrap();
        
        // Only removes "bad" when it appears at the beginning of a line
        let expected = "start of line.\nMiddle bad word.\nger beginning.\nNormal line.";
        assert_eq!(result["text"], expected);
    }

    #[test]
    fn test_process_suffix_location() {
        let config = json!({
            "banlist": "bad",
            "location": "suffix"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("End of line bad\nMiddle bad word.\nEnding in bad\nNormal line.");
        let result = processor.process(input).unwrap().unwrap();
        
        // Only removes "bad" when it appears at the end of a line
        let expected = "End of line\nMiddle bad word.\nEnding in\nNormal line.";
        assert_eq!(result["text"], expected);
    }

    #[test]
    fn test_empty_lines_after_removal() {
        let config = json!({
            "banlist": "bad",
            "remove_substring_only": true
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("bad\nThis is good.\nbad bad");
        let result = processor.process(input).unwrap().unwrap();
        
        // The first "bad" line will become empty after removal and should be skipped
        // The third "bad bad" line will become "  " and after trimming should be skipped
        let expected = "This is good.";
        assert_eq!(result["text"], expected);
    }

    #[test]
    fn test_multi_word_phrase() {
        let config = json!({
            "banlist": "very bad",
            "remove_substring_only": true
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("This is very bad\nThis is bad only.\nThis is very very bad indeed.");
        let result = processor.process(input).unwrap().unwrap();
        
        // Only the exact phrase "very bad" should be removed
        let expected = "This is \nThis is bad only.\nThis is very indeed.";
        assert_eq!(result["text"], expected);
    }

    #[test]
    fn test_empty_text() {
        let config = json!({
            "banlist": "bad"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("");
        let result = processor.process(input).unwrap().unwrap();
        
        assert_eq!(result["text"], "");
    }

    #[test]
    fn test_no_matches() {
        let config = json!({
            "banlist": "nonexistent"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("This text has no banned words.\nAll clean here!");
        let result = processor.process(input).unwrap().unwrap();
        
        // Text should remain unchanged
        assert_eq!(result["text"], "This text has no banned words.\nAll clean here!");
    }

    #[test]
    fn test_case_sensitivity() {
        let config = json!({
            "banlist": "Bad"
        });

        let processor = SubstringLineModifier::new(&config).unwrap();
        let input = create_test_data("This contains Bad.\nThis contains bad.");
        let result = processor.process(input).unwrap().unwrap();
        
        // The regex in the implementation would make this case-sensitive
        // Only exact "Bad" should be removed, not "bad"
        let expected = "This contains .\nThis contains bad.";
        assert_eq!(result["text"], expected);
    }
}