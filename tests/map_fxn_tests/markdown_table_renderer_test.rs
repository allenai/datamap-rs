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