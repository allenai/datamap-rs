extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, JqAnnotator};
use serde_json::json;

#[test]
fn test_jq_extract_field() {
    // Use a jq expression to extract and transform a field
    let config = json!({
        "expression": ".name | ascii_downcase",
        "output_field": "name_lower"
    });
    let annotator = JqAnnotator::new(&config).unwrap();

    let data = json!({
        "name": "HELLO WORLD",
        "value": 42
    });

    let result = annotator.process(data).unwrap().unwrap();
    assert_eq!(result["name"], "HELLO WORLD"); // original preserved
    assert_eq!(result["name_lower"], "hello world");
    assert_eq!(result["value"], 42);
}

#[test]
fn test_jq_compute_expression_multiple_docs() {
    // Process three different documents with the same annotator to verify
    // the compiled filter is reusable and not stateful across calls
    let config = json!({
        "expression": "(.a + .b) * .c",
        "output_field": "result"
    });
    let annotator = JqAnnotator::new(&config).unwrap();

    let d1 = json!({"a": 2, "b": 3, "c": 10});
    let d2 = json!({"a": 0, "b": 0, "c": 99});
    let d3 = json!({"a": -1, "b": 4, "c": 5});

    // Process all three before checking any results
    let r1 = annotator.process(d1).unwrap().unwrap();
    let r2 = annotator.process(d2).unwrap().unwrap();
    let r3 = annotator.process(d3).unwrap().unwrap();

    assert_eq!(r1["result"], 50);  // (2+3)*10
    assert_eq!(r2["result"], 0);   // (0+0)*99
    assert_eq!(r3["result"], 15);  // (-1+4)*5

    // original fields preserved in each
    assert_eq!(r1["a"], 2);
    assert_eq!(r2["a"], 0);
    assert_eq!(r3["c"], 5);
}

#[test]
fn test_jq_build_object() {
    // Use a jq expression that builds a new object from input fields
    let config = json!({
        "expression": "{len: (.text | length), words: (.text | split(\" \") | length)}",
        "output_field": "stats"
    });
    let annotator = JqAnnotator::new(&config).unwrap();

    let data = json!({
        "id": "doc1",
        "text": "hello world foo"
    });

    let result = annotator.process(data).unwrap().unwrap();
    assert_eq!(result["id"], "doc1");
    assert_eq!(result["text"], "hello world foo");
    assert_eq!(result["stats"]["len"], 15);
    assert_eq!(result["stats"]["words"], 3);
}

#[test]
fn test_jq_concatenate_fields() {
    // Use a jq expression to concatenate string fields with a separator
    let config = json!({
        "expression": ".first_name + \" \" + .last_name",
        "output_field": "full_name"
    });
    let annotator = JqAnnotator::new(&config).unwrap();

    let data = json!({
        "first_name": "Jane",
        "last_name": "Doe",
        "age": 30
    });

    let result = annotator.process(data).unwrap().unwrap();
    assert_eq!(result["full_name"], "Jane Doe");
    assert_eq!(result["first_name"], "Jane");
    assert_eq!(result["last_name"], "Doe");
    assert_eq!(result["age"], 30);
}
