extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, NonNullFilter};
use serde_json::json;

#[test]
fn test_non_null_filter_basic() {
    // Test basic non-null filtering
    let config = json!({
        "attribute_field": "status"
    });

    let filter = NonNullFilter::new(&config).unwrap();

    // Document with present non-null value should pass
    let data1 = json!({
        "text": "Some text",
        "status": "active"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with null value should be filtered out
    let data2 = json!({
        "text": "Some text",
        "status": null
    });
    assert!(filter.process(data2).unwrap().is_none());

    // Document with missing attribute should be filtered out
    let data3 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data3).unwrap().is_none());
}

#[test]
fn test_non_null_filter_different_types() {
    // Test that NonNullFilter works with different data types
    let config = json!({
        "attribute_field": "value"
    });

    let filter = NonNullFilter::new(&config).unwrap();

    // String value should pass
    let data1 = json!({
        "value": "text"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Number value should pass
    let data2 = json!({
        "value": 42
    });
    assert!(filter.process(data2).unwrap().is_some());

    // Boolean value should pass
    let data3 = json!({
        "value": true
    });
    assert!(filter.process(data3).unwrap().is_some());

    // Array value should pass
    let data4 = json!({
        "value": [1, 2, 3]
    });
    assert!(filter.process(data4).unwrap().is_some());

    // Object value should pass
    let data5 = json!({
        "value": {"nested": "object"}
    });
    assert!(filter.process(data5).unwrap().is_some());

    // Empty string should pass (it's not null)
    let data6 = json!({
        "value": ""
    });
    assert!(filter.process(data6).unwrap().is_some());

    // Zero should pass (it's not null)
    let data7 = json!({
        "value": 0
    });
    assert!(filter.process(data7).unwrap().is_some());

    // False should pass (it's not null)
    let data8 = json!({
        "value": false
    });
    assert!(filter.process(data8).unwrap().is_some());

    // Empty array should pass (it's not null)
    let data9 = json!({
        "value": []
    });
    assert!(filter.process(data9).unwrap().is_some());

    // Empty object should pass (it's not null)
    let data10 = json!({
        "value": {}
    });
    assert!(filter.process(data10).unwrap().is_some());
}

#[test]
fn test_non_null_filter_nested_attributes() {
    // Test with nested attribute paths
    let config = json!({
        "attribute_field": "metadata.author.name"
    });

    let filter = NonNullFilter::new(&config).unwrap();

    // Document with complete nested structure should pass
    let data1 = json!({
        "text": "Some text",
        "metadata": {
            "author": {
                "name": "John Doe"
            }
        }
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with null nested value should be filtered out
    let data2 = json!({
        "text": "Some text",
        "metadata": {
            "author": {
                "name": null
            }
        }
    });
    assert!(filter.process(data2).unwrap().is_none());

    // Document with missing nested field should be filtered out
    let data3 = json!({
        "text": "Some text",
        "metadata": {
            "author": {}
        }
    });
    assert!(filter.process(data3).unwrap().is_none());

    // Document with missing intermediate structure should be filtered out
    let data4 = json!({
        "text": "Some text",
        "metadata": {}
    });
    assert!(filter.process(data4).unwrap().is_none());

    // Document with missing top-level structure should be filtered out
    let data5 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data5).unwrap().is_none());
}

#[test]
fn test_non_null_filter_with_arrays() {
    // Test that the filter can check if array fields themselves are non-null
    // Note: json_get doesn't support array indexing like "items.0.id"
    let config = json!({
        "attribute_field": "items"
    });

    let filter = NonNullFilter::new(&config).unwrap();

    // Document with non-null array should pass
    let data1 = json!({
        "items": [
            {"id": "item1"},
            {"id": "item2"}
        ]
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with empty array should pass (empty array is not null)
    let data2 = json!({
        "items": []
    });
    assert!(filter.process(data2).unwrap().is_some());

    // Document with null array should be filtered out
    let data3 = json!({
        "items": null
    });
    assert!(filter.process(data3).unwrap().is_none());

    // Document with missing array field should be filtered out
    let data4 = json!({
        "other_field": "value"
    });
    assert!(filter.process(data4).unwrap().is_none());
}

#[test]
fn test_non_null_filter_missing_config() {
    // Test that missing configuration produces an error
    let config = json!({});
    
    assert!(NonNullFilter::new(&config).is_err());
}

#[test]
fn test_non_null_filter_invalid_config() {
    // Test that invalid attribute_field type produces an error
    let config = json!({
        "attribute_field": 123  // Should be a string
    });
    
    assert!(NonNullFilter::new(&config).is_err());
}

#[test]
fn test_non_null_filter_multiple_documents() {
    // Test filtering multiple documents
    let config = json!({
        "attribute_field": "category"
    });

    let filter = NonNullFilter::new(&config).unwrap();

    let documents = vec![
        json!({"id": 1, "category": "sports"}),
        json!({"id": 2, "category": null}),
        json!({"id": 3, "text": "no category"}),
        json!({"id": 4, "category": "tech"}),
        json!({"id": 5, "category": ""}),
        json!({"id": 6, "category": 0}),
        json!({"id": 7, "category": false}),
    ];

    let expected_results = vec![
        true,   // has category "sports"
        false,  // category is null
        false,  // category missing
        true,   // has category "tech"
        true,   // empty string is not null
        true,   // 0 is not null
        true,   // false is not null
    ];

    for (doc, expected) in documents.into_iter().zip(expected_results) {
        let result = filter.process(doc).unwrap();
        assert_eq!(result.is_some(), expected);
    }
}