extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, HtmlEditor};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ==================== Constructor Tests ====================

    #[test]
    fn test_html_editor_new_defaults() {
        let config = json!({
            "tag": "footnote"
        });
        let editor = HtmlEditor::new(&config).unwrap();
        assert_eq!(editor.text_field, "text");
        assert_eq!(editor.tag, "footnote");
        assert_eq!(editor.action, "remove");
        assert_eq!(editor.max_ratio, 0.5);
    }

    #[test]
    fn test_html_editor_new_custom_values() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.3,
            "text_field": "content"
        });
        let editor = HtmlEditor::new(&config).unwrap();
        assert_eq!(editor.text_field, "content");
        assert_eq!(editor.tag, "table");
        assert_eq!(editor.action, "filter_by_total_chars");
        assert_eq!(editor.max_ratio, 0.3);
    }

    #[test]
    fn test_html_editor_new_missing_tag() {
        let config = json!({
            "action": "remove"
        });
        let result = HtmlEditor::new(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("tag"));
    }

    #[test]
    fn test_html_editor_new_invalid_action() {
        let config = json!({
            "tag": "footnote",
            "action": "invalid_action"
        });
        let result = HtmlEditor::new(&config);
        assert!(result.is_err());
    }

    // ==================== Remove Action Tests ====================

    #[test]
    fn test_remove_footnote_tags() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Hello world<footnote>This is a footnote</footnote> and more text."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Hello world and more text.");
    }

    #[test]
    fn test_remove_multiple_footnotes() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "First<footnote>note1</footnote> and second<footnote>note2</footnote> end."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "First and second end.");
    }

    #[test]
    fn test_remove_nested_html_in_tag() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Text<footnote><b>bold</b> and <i>italic</i></footnote> more."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Text more.");
    }

    #[test]
    fn test_remove_tag_with_attributes() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Text<footnote id=\"1\" class=\"note\">content</footnote> end."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Text end.");
    }

    #[test]
    fn test_remove_case_insensitive() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Text<FOOTNOTE>upper</FOOTNOTE> and <Footnote>mixed</Footnote> end."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Text and  end.");
    }

    #[test]
    fn test_remove_multiline_tag() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Text<footnote>\nline1\nline2\n</footnote> end."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Text end.");
    }

    #[test]
    fn test_remove_no_matching_tags() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Hello world with no footnotes."
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Hello world with no footnotes.");
    }

    // ==================== Filter by Total Chars Tests ====================
    // filter_by_total_chars compares: (all chars inside <tag>...</tag>) / (total doc chars)

    #[test]
    fn test_filter_by_total_chars_below_threshold() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.8
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // "<table><tr><td>small</td></tr></table>" = 38 chars
        // Total doc = 58 chars
        // Ratio = 38/58 = 0.655, below 0.8 threshold
        let data = json!({
            "text": "Some text <table><tr><td>small</td></tr></table> more text"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_filter_by_total_chars_above_threshold() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.5
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // "<table><tr><td>small</td></tr></table>" = 38 chars
        // Total doc = 58 chars
        // Ratio = 38/58 = 0.655, above 0.5 threshold
        let data = json!({
            "text": "Some text <table><tr><td>small</td></tr></table> more text"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_filter_by_total_chars_multiple_tables() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.4
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // "<table>AAA</table>" = 18 chars, "<table>BBB</table>" = 18 chars = 36 total
        // Total doc = 43 chars
        // Ratio = 36/43 = 0.84, above 0.4
        let data = json!({
            "text": "X <table>AAA</table> Y <table>BBB</table> Z"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_filter_by_total_chars_empty_table() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.5
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // "<table></table>" = 15 chars
        // Total doc = 40 chars
        // Ratio = 15/40 = 0.375, below 0.5
        let data = json!({
            "text": "Some text <table></table> more text here"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_filter_by_total_chars_no_tables() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.1
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Just plain text without any tables at all."
        });

        let result = editor.process(data).unwrap();
        // No tables means 0 chars in tables, ratio is 0
        assert!(result.is_some());
    }

    // ==================== Filter by HTML Chars Tests ====================
    // filter_by_html_chars compares: (HTML markup chars inside <tag>...</tag>) / (total doc chars)

    #[test]
    fn test_filter_by_html_chars_below_threshold() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_html_chars",
            "max_ratio": 0.5
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // Table with substantial text content, relatively less markup
        // Markup: <table>, <tr>, <td>, </td>, </tr>, </table> = ~33 chars
        // Total doc length is much larger due to text content
        let data = json!({
            "text": "Intro <table><tr><td>This cell has a lot of meaningful text content that makes the markup ratio low</td></tr></table>"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_filter_by_html_chars_above_threshold() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_html_chars",
            "max_ratio": 0.3
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // Table with many empty cells - lots of markup, little text
        // Total doc is short, so markup ratio is high
        let data = json!({
            "text": "X <table><tr><td></td><td></td><td></td></tr><tr><td></td><td></td><td></td></tr></table>"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_filter_by_html_chars_counts_all_markup() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_html_chars",
            "max_ratio": 0.1
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // Nested tags should all be counted
        // Many tags: <table>, <thead>, <tr>, <th>, </th>, </tr>, </thead>, <tbody>, <tr>, <td>, </td>, </tr>, </tbody>, </table>
        let data = json!({
            "text": "Text <table><thead><tr><th>H</th></tr></thead><tbody><tr><td>D</td></tr></tbody></table> end"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_filter_by_html_chars_no_tables() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_html_chars",
            "max_ratio": 0.1
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Just plain text without any tables at all."
        });

        let result = editor.process(data).unwrap();
        // No tables means 0 HTML chars in tables, ratio is 0
        assert!(result.is_some());
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_empty_text() {
        let config = json!({
            "tag": "footnote",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": ""
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "");
    }

    #[test]
    fn test_filter_empty_text() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_total_chars",
            "max_ratio": 0.5
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": ""
        });

        // Empty text should pass (no division by zero)
        let result = editor.process(data).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_different_tag_types() {
        // Test with div tag
        let config = json!({
            "tag": "div",
            "action": "remove"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "text": "Before <div class=\"sidebar\">sidebar content</div> after"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["text"].as_str().unwrap(), "Before  after");
    }

    #[test]
    fn test_custom_text_field() {
        let config = json!({
            "tag": "footnote",
            "action": "remove",
            "text_field": "content"
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({
            "content": "Hello<footnote>note</footnote> world",
            "text": "This should be ignored"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
        let result_val = result.unwrap();
        assert_eq!(result_val["content"].as_str().unwrap(), "Hello world");
        assert_eq!(result_val["text"].as_str().unwrap(), "This should be ignored");
    }

    #[test]
    fn test_ratio_at_exact_threshold() {
        let config = json!({
            "tag": "b",
            "action": "filter_by_total_chars",
            "max_ratio": 0.5
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // "<b>YY</b>" = 9 chars, "XXXXXXXXX" = 9 chars
        // Total = 18 chars, ratio = 9/18 = 0.5
        let data = json!({
            "text": "XXXXXXXXX<b>YY</b>"
        });

        let result = editor.process(data).unwrap();
        // At exact threshold (0.5 <= 0.5), should pass
        assert!(result.is_some());
    }

    #[test]
    fn test_ratio_just_above_threshold() {
        let config = json!({
            "tag": "b",
            "action": "filter_by_total_chars",
            "max_ratio": 0.5
        });
        let editor = HtmlEditor::new(&config).unwrap();

        // "<b>YYY</b>" = 10 chars, "XXXXXXXX" = 8 chars
        // Total = 18 chars, ratio = 10/18 = 0.556 > 0.5
        let data = json!({
            "text": "XXXXXXXX<b>YYY</b>"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_real_doc1() {
        let config = json!({
            "tag": "table",
            "action": "filter_by_html_chars",
            "max_ratio": 0.2
        });
        let editor = HtmlEditor::new(&config).unwrap();

        let data = json!({ "text": 
      "PUMP STATION NO. 6 (ALGODON CANAL)

DITCH FILL POINT TABLE

<table>
  <tr>
    <th>ST. NO.</th>
    <th>NORTHING</th>
    <th>EASTING</th>
    <th>ELEV.</th>
    <th>OFFSET</th>
    <th>EXPLANATION</th>
    <th>SECTION</th>
  </tr>
  <!-- Table rows omitted for brevity -->
</table>

DITCH FILL POINT TABLE

<table>
  <tr>
    <th>ST. NO.</th>
    <th>NORTHING</th>
    <th>EASTING</th>
    <th>ELEV.</th>
    <th>OFFSET</th>
    <th>EXPLANATION</th>
    <th>SECTION</th>
  </tr>
  <!-- Table rows omitted for brevity -->
</table>

NOTES:
1. SEEVEE ELEVATION IS IN FEET LEVEE ELEV. (ELA) 2 DRAWING SCALE.
2. SEEVEE ELEVATIONS ARE 3' ABOVE GRADE WHERE SEEVEE BERM IS LOCATED.

Phase II Levee Repairs
Upper Bear River, WP Interceptor Canal and Yuba River
Reclamation District No. 784
Marysville, California
Three Rivers Levee Improvement Authority

UPPER BEAR RIVER
SEEVEE BERM
STATION 141+00 TO 144+50

HDR Engineering, Inc.
A. JOHNSON
A. HALL
A. COLLINS"
        });

        let result = editor.process(data).unwrap();
        assert!(result.is_some());
    }
}
