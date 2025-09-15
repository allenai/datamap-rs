extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, AllowListFilter, DenyListFilter};
use serde_json::json;
use std::fs::File;
use std::io::Write;
use std::env;

#[test]
fn test_allow_list_filter() {
    // Create a temporary directory and allow list file
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_allow_list.txt");
        let mut file = File::create(&allow_list_path).unwrap();
        writeln!(file, "python").unwrap();
        writeln!(file, "rust").unwrap();
        writeln!(file, "javascript").unwrap();
        writeln!(file, "").unwrap(); // Empty line should be ignored
        writeln!(file, "  typescript  ").unwrap(); // Should be trimmed

        // Test with string attribute
        let config = json!({
            "attribute_field": "metadata.language",
            "allow_list_file": allow_list_path.to_str().unwrap()
        });

        let filter = AllowListFilter::new(&config).unwrap();

        // Test allowed language
        let data1 = json!({
            "text": "Some code",
            "metadata": {
                "language": "python"
            }
        });
        assert!(filter.process(data1).unwrap().is_some());

        // Test trimmed language
        let data2 = json!({
            "text": "Some code",
            "metadata": {
                "language": "typescript"
            }
        });
        assert!(filter.process(data2).unwrap().is_some());

        // Test disallowed language
        let data3 = json!({
            "text": "Some code",
            "metadata": {
                "language": "java"
            }
        });
        assert!(filter.process(data3).unwrap().is_none());

        // Test with numeric attribute
        let allow_list_path2 = temp_dir.join("test_allow_list2.txt");
        let mut file2 = File::create(&allow_list_path2).unwrap();
        writeln!(file2, "1").unwrap();
        writeln!(file2, "2").unwrap();
        writeln!(file2, "3").unwrap();

        let config2 = json!({
            "attribute_field": "id",
            "allow_list_file": allow_list_path2.to_str().unwrap()
        });

        let filter2 = AllowListFilter::new(&config2).unwrap();

        let data4 = json!({
            "text": "Some text",
            "id": 2
        });
        assert!(filter2.process(data4).unwrap().is_some());

        let data5 = json!({
            "text": "Some text",
            "id": 5
        });
        assert!(filter2.process(data5).unwrap().is_none());
    }

#[test]
fn test_deny_list_filter() {
    // Create a temporary directory and deny list file
    let temp_dir = env::temp_dir();
    let deny_list_path = temp_dir.join("test_deny_list.txt");
        let mut file = File::create(&deny_list_path).unwrap();
        writeln!(file, "spam").unwrap();
        writeln!(file, "adult").unwrap();
        writeln!(file, "gambling").unwrap();
        writeln!(file, "").unwrap(); // Empty line should be ignored
        writeln!(file, "  phishing  ").unwrap(); // Should be trimmed

        // Test with string attribute
        let config = json!({
            "attribute_field": "metadata.category",
            "deny_list_file": deny_list_path.to_str().unwrap()
        });

        let filter = DenyListFilter::new(&config).unwrap();

        // Test allowed category
        let data1 = json!({
            "text": "Some content",
            "metadata": {
                "category": "education"
            }
        });
        assert!(filter.process(data1).unwrap().is_some());

        // Test denied category
        let data2 = json!({
            "text": "Some content",
            "metadata": {
                "category": "spam"
            }
        });
        assert!(filter.process(data2).unwrap().is_none());

        // Test trimmed denied category
        let data3 = json!({
            "text": "Some content",
            "metadata": {
                "category": "phishing"
            }
        });
        assert!(filter.process(data3).unwrap().is_none());

        // Test with boolean attribute
        let deny_list_path2 = temp_dir.join("test_deny_list2.txt");
        let mut file2 = File::create(&deny_list_path2).unwrap();
        writeln!(file2, "true").unwrap();

        let config2 = json!({
            "attribute_field": "is_spam",
            "deny_list_file": deny_list_path2.to_str().unwrap()
        });

        let filter2 = DenyListFilter::new(&config2).unwrap();

        let data4 = json!({
            "text": "Some text",
            "is_spam": true
        });
        assert!(filter2.process(data4).unwrap().is_none());

        let data5 = json!({
            "text": "Some text",
            "is_spam": false
        });
        assert!(filter2.process(data5).unwrap().is_some());
    }

#[test]
fn test_nested_attribute_access() {
    // Test that nested attributes work correctly
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_nested_allow_list.txt");
        let mut file = File::create(&allow_list_path).unwrap();
        writeln!(file, "en").unwrap();
        writeln!(file, "es").unwrap();
        writeln!(file, "fr").unwrap();

        let config = json!({
            "attribute_field": "metadata.document.language",
            "allow_list_file": allow_list_path.to_str().unwrap()
        });

        let filter = AllowListFilter::new(&config).unwrap();

        let data = json!({
            "text": "Some text",
            "metadata": {
                "document": {
                    "language": "en"
                }
            }
        });
        assert!(filter.process(data).unwrap().is_some());

        let data2 = json!({
            "text": "Some text",
            "metadata": {
                "document": {
                    "language": "de"
                }
            }
        });
        assert!(filter.process(data2).unwrap().is_none());
    }

#[test]
fn test_missing_attribute_error() {
    // Test that missing attributes produce an error with default on_null behavior
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_missing_allow_list.txt");
        let mut file = File::create(&allow_list_path).unwrap();
        writeln!(file, "value").unwrap();

        let config = json!({
            "attribute_field": "missing_field",
            "allow_list_file": allow_list_path.to_str().unwrap()
        });

        let filter = AllowListFilter::new(&config).unwrap();

        let data = json!({
            "text": "Some text"
        });
        
        // With default on_null="remove", missing attribute should filter out the document
        assert!(filter.process(data).unwrap().is_none());
}

#[test]
fn test_allow_list_filter_on_null_remove() {
    // Test AllowListFilter with on_null: remove (default behavior)
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_allow_on_null_remove.txt");
    let mut file = File::create(&allow_list_path).unwrap();
    writeln!(file, "approved").unwrap();
    writeln!(file, "verified").unwrap();

    let config = json!({
        "attribute_field": "status",
        "allow_list_file": allow_list_path.to_str().unwrap(),
        "on_null": "remove"
    });

    let filter = AllowListFilter::new(&config).unwrap();

    // Document with allowed value should pass
    let data1 = json!({
        "text": "Some text",
        "status": "approved"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with null value should be removed
    let data2 = json!({
        "text": "Some text",
        "status": null
    });
    assert!(filter.process(data2).unwrap().is_none());

    // Document with missing attribute should be removed
    let data3 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data3).unwrap().is_none());

    // Document with non-allowed value should be removed
    let data4 = json!({
        "text": "Some text",
        "status": "pending"
    });
    assert!(filter.process(data4).unwrap().is_none());
}

#[test]
fn test_allow_list_filter_on_null_keep() {
    // Test AllowListFilter with on_null: keep
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_allow_on_null_keep.txt");
    let mut file = File::create(&allow_list_path).unwrap();
    writeln!(file, "approved").unwrap();
    writeln!(file, "verified").unwrap();

    let config = json!({
        "attribute_field": "status",
        "allow_list_file": allow_list_path.to_str().unwrap(),
        "on_null": "keep"
    });

    let filter = AllowListFilter::new(&config).unwrap();

    // Document with allowed value should pass
    let data1 = json!({
        "text": "Some text",
        "status": "approved"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with null value should be kept
    let data2 = json!({
        "text": "Some text",
        "status": null
    });
    assert!(filter.process(data2).unwrap().is_some());

    // Document with missing attribute should be kept
    let data3 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data3).unwrap().is_some());

    // Document with non-allowed value should still be removed
    let data4 = json!({
        "text": "Some text",
        "status": "pending"
    });
    assert!(filter.process(data4).unwrap().is_none());
}

#[test]
fn test_deny_list_filter_on_null_remove() {
    // Test DenyListFilter with on_null: remove (default behavior)
    let temp_dir = env::temp_dir();
    let deny_list_path = temp_dir.join("test_deny_on_null_remove.txt");
    let mut file = File::create(&deny_list_path).unwrap();
    writeln!(file, "spam").unwrap();
    writeln!(file, "blocked").unwrap();

    let config = json!({
        "attribute_field": "category",
        "deny_list_file": deny_list_path.to_str().unwrap(),
        "on_null": "remove"
    });

    let filter = DenyListFilter::new(&config).unwrap();

    // Document with non-denied value should pass
    let data1 = json!({
        "text": "Some text",
        "category": "valid"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with denied value should be removed
    let data2 = json!({
        "text": "Some text",
        "category": "spam"
    });
    assert!(filter.process(data2).unwrap().is_none());

    // Document with null value should be removed
    let data3 = json!({
        "text": "Some text",
        "category": null
    });
    assert!(filter.process(data3).unwrap().is_none());

    // Document with missing attribute should be removed
    let data4 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data4).unwrap().is_none());
}

#[test]
fn test_deny_list_filter_on_null_keep() {
    // Test DenyListFilter with on_null: keep
    let temp_dir = env::temp_dir();
    let deny_list_path = temp_dir.join("test_deny_on_null_keep.txt");
    let mut file = File::create(&deny_list_path).unwrap();
    writeln!(file, "spam").unwrap();
    writeln!(file, "blocked").unwrap();

    let config = json!({
        "attribute_field": "category",
        "deny_list_file": deny_list_path.to_str().unwrap(),
        "on_null": "keep"
    });

    let filter = DenyListFilter::new(&config).unwrap();

    // Document with non-denied value should pass
    let data1 = json!({
        "text": "Some text",
        "category": "valid"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with denied value should be removed
    let data2 = json!({
        "text": "Some text",
        "category": "spam"
    });
    assert!(filter.process(data2).unwrap().is_none());

    // Document with null value should be kept
    let data3 = json!({
        "text": "Some text",
        "category": null
    });
    assert!(filter.process(data3).unwrap().is_some());

    // Document with missing attribute should be kept
    let data4 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data4).unwrap().is_some());
}

#[test]
fn test_on_null_nested_attribute() {
    // Test on_null behavior with nested attributes
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_nested_on_null.txt");
    let mut file = File::create(&allow_list_path).unwrap();
    writeln!(file, "en").unwrap();

    // Test with on_null: keep
    let config = json!({
        "attribute_field": "metadata.language",
        "allow_list_file": allow_list_path.to_str().unwrap(),
        "on_null": "keep"
    });

    let filter = AllowListFilter::new(&config).unwrap();

    // Document with missing nested structure should be kept
    let data1 = json!({
        "text": "Some text"
    });
    assert!(filter.process(data1).unwrap().is_some());

    // Document with partial nested structure should be kept
    let data2 = json!({
        "text": "Some text",
        "metadata": {}
    });
    assert!(filter.process(data2).unwrap().is_some());

    // Document with null nested value should be kept
    let data3 = json!({
        "text": "Some text",
        "metadata": {
            "language": null
        }
    });
    assert!(filter.process(data3).unwrap().is_some());
}

#[test]
fn test_invalid_on_null_value() {
    // Test that invalid on_null values are rejected
    let temp_dir = env::temp_dir();
    let allow_list_path = temp_dir.join("test_invalid_on_null.txt");
    let mut file = File::create(&allow_list_path).unwrap();
    writeln!(file, "value").unwrap();

    let config = json!({
        "attribute_field": "field",
        "allow_list_file": allow_list_path.to_str().unwrap(),
        "on_null": "invalid"
    });

    // Should fail to create filter with invalid on_null value
    assert!(AllowListFilter::new(&config).is_err());

    let config2 = json!({
        "attribute_field": "field",
        "deny_list_file": allow_list_path.to_str().unwrap(),
        "on_null": "invalid"
    });

    assert!(DenyListFilter::new(&config2).is_err());
}