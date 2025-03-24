extern crate datamap_rs;
use serde_json::json;
//use datamap_rs::{DataProcessor};
use datamap_rs::map_fxn::{DataProcessor, TextLenFilter};

//use datamap_rs::map_fxn::TextLenFilter;


#[test]
fn test_text_len_filter_within_bounds() {
    // Arrange
    let config = json!({
        "text_field": "content",
        "lower_bound": 5,
        "upper_bound": 20
    });
    let filter = TextLenFilter::new(&config).unwrap();
    
    // Test data within bounds
    let data = json!({
        "id": "doc1",
        "content": "This is valid text"  // 17 characters, within [5, 20]
    });
    
    // Act
    let result = filter.process(data.clone()).unwrap();
    
    // Assert
    assert!(result.is_some());
    assert_eq!(result.unwrap(), data);
}

#[test]
fn test_text_len_filter_below_lower_bound() {
    // Arrange
    let config = json!({
        "text_field": "content",
        "lower_bound": 10,
        "upper_bound": 100
    });
    let filter = TextLenFilter::new(&config).unwrap();
    
    // Test data below lower bound
    let data = json!({
        "id": "doc2",
        "content": "Short"  // 5 characters, below lower bound of 10
    });
    
    // Act
    let result = filter.process(data).unwrap();
    
    // Assert
    assert!(result.is_none());
}

#[test]
fn test_text_len_filter_above_upper_bound() {
    // Arrange
    let config = json!({
        "text_field": "content",
        "lower_bound": 0,
        "upper_bound": 10
    });
    let filter = TextLenFilter::new(&config).unwrap();
    
    // Test data above upper bound
    let data = json!({
        "id": "doc3",
        "content": "This text is too long for the filter"  // 36 characters, above upper bound of 10
    });
    
    // Act
    let result = filter.process(data).unwrap();
    
    // Assert
    assert!(result.is_none());
}