use serde_json::json;
use datamap_rs::map_fxn::{DataProcessor, MarkdownTableRenderer};


#[test]
fn test_markdown_table_renderer() {
    let config = json!({
        "text_field": "text"
    });


    let processor = MarkdownTableRenderer::new(&config).unwrap();


    let input_data = json!({
        "text": "This is some text with a markdown table:\n\n| Column 1 | Column 2 | Column 3 |\n|----------|----------|----------|\n| Cell 1   | Cell 2   | Cell 3   |\n| Cell 4   | Cell 5   | Cell 6   |\n\nAnd some more text with *italics* and **bold** that should remain unchanged."
    });


    let result = processor.process(input_data).unwrap().unwrap();
    let processed_text = result["text"].as_str().unwrap();


    // Check that the table was converted to HTML
    assert!(processed_text.contains("<table>"));
    assert!(processed_text.contains("<thead>"));
    assert!(processed_text.contains("<tbody>"));
    assert!(processed_text.contains("<th>Column 1</th>"));
    assert!(processed_text.contains("<td>Cell 1</td>"));


    // Check that other markdown syntax remains unchanged
    assert!(processed_text.contains("*italics*"));
    assert!(processed_text.contains("**bold**"));
}


#[test]
fn test_markdown_table_renderer_no_tables() {
    let config = json!({
        "text_field": "text"
    });


    let processor = MarkdownTableRenderer::new(&config).unwrap();


    let input_data = json!({
        "text": "This is just regular text with *italics* and **bold** but no tables."
    });


    let result = processor.process(input_data).unwrap().unwrap();
    let processed_text = result["text"].as_str().unwrap();


    // Check that text remains unchanged when no tables present
    assert_eq!(processed_text, "This is just regular text with *italics* and **bold** but no tables.");
}


#[test]
fn test_markdown_table_renderer_multiple_tables() {
    let config = json!({
        "text_field": "text"
    });


    let processor = MarkdownTableRenderer::new(&config).unwrap();


    let input_data = json!({
        "text": "First table:\n\n| A | B |\n|---|---|\n| 1 | 2 |\n\nSome text between tables.\n\n| X | Y |\n|---|---|\n| 3 | 4 |\n\nEnd text."
    });


    let result = processor.process(input_data).unwrap().unwrap();
    let processed_text = result["text"].as_str().unwrap();


    // Check that both tables were converted
    let table_count = processed_text.matches("<table>").count();
    assert_eq!(table_count, 2);


    // Check that content between tables is preserved
    assert!(processed_text.contains("Some text between tables."));
    assert!(processed_text.contains("End text."));
}


#[test]
fn test_markdown_table_renderer_minimum_two_lines() {
    let config = json!({
        "text_field": "text"
    });


    let processor = MarkdownTableRenderer::new(&config).unwrap();


    // Test with single line starting with | - should NOT be converted to HTML
    let single_line_data = json!({
        "text": "Some text\n| Single line with pipe\nMore text"
    });


    let result = processor.process(single_line_data).unwrap().unwrap();
    let processed_text = result["text"].as_str().unwrap();


    // Should keep original pipe line
    assert_eq!(processed_text, "Some text\n| Single line with pipe\nMore text");


    // Test with two lines starting with | - SHOULD be converted to HTML (proper table format)
    let two_line_data = json!({
        "text": "Some text\n| Header 1 | Header 2 |\n|----------|----------|\n| Cell 1   | Cell 2   |\nMore text"
    });


    let result2 = processor.process(two_line_data).unwrap().unwrap();
    let processed_text2 = result2["text"].as_str().unwrap();


    // Should contain HTML table elements
    assert!(processed_text2.contains("<table>"));
    assert!(processed_text2.contains("Header 1"));
    assert!(processed_text2.contains("Cell 1"));


    // Test with exactly two lines starting with | (no separator) - should still be processed but may not become a table
    let two_lines_no_separator = json!({
        "text": "Some text\n| Line 1 with pipe |\n| Line 2 with pipe |\nMore text"
    });


    let result3 = processor.process(two_lines_no_separator).unwrap().unwrap();
    let processed_text3 = result3["text"].as_str().unwrap();


    // Should be processed through markdown (even if it doesn't become a proper table)
    // The key is that it gets processed, not necessarily that it becomes a table
    assert!(processed_text3.contains("Line 1 with pipe"));
    assert!(processed_text3.contains("Line 2 with pipe"));
}


#[test]
fn test_markdown_table_renderer_requires_start_and_end_pipes() {
    let config = json!({
        "text_field": "text"
    });


    let processor = MarkdownTableRenderer::new(&config).unwrap();


    // Test with lines that start with | but don't end with | - should NOT be processed as table
    let incomplete_pipes_data = json!({
        "text": "Some text\n| Line starts with pipe but doesn't end\n| Another line starts with pipe\nMore text"
    });


    let result = processor.process(incomplete_pipes_data).unwrap().unwrap();
    let processed_text = result["text"].as_str().unwrap();


    // Should NOT be processed as table, should remain unchanged
    assert!(!processed_text.contains("<table>"));
    assert_eq!(processed_text, "Some text\n| Line starts with pipe but doesn't end\n| Another line starts with pipe\nMore text");


    let input_data = json!({
        "text": "This is some text with a markdown table:\n\n| Column 1 | Column 2 | Column 3   |  \n|----------|----------|----------|\t\n | Cell 1   | Cell 2   | Cell 3   |\n| Cell 4   | Cell 5   | Cell 6   |  \n\n"
    });


    let result = processor.process(input_data).unwrap().unwrap();
    let processed_text = result["text"].as_str().unwrap();


    // Check that the table was converted to HTML
    assert!(processed_text.contains("<table>"));
    assert!(processed_text.contains("<thead>"));
    assert!(processed_text.contains("<tbody>"));
    assert!(processed_text.contains("<th>Column 1</th>"));
    assert!(processed_text.contains("<td>Cell 1</td>"));
}