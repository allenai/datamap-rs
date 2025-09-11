extern crate datamap_rs;
use datamap_rs::map_fxn::{DataProcessor, FastTextAnnotator};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    
    
  
    // Helper function to create a basic config
    fn create_basic_config(fast_text_file: &str) -> Value {
        json!({
            "fast_text_file": fast_text_file
        })
    }
    
    // Helper function to create a comprehensive config
    fn create_full_config(fast_text_file: &str) -> Value {
        json!({
            "fast_text_file": fast_text_file,
            "text_field": "content.body",
            "output_field": "metadata.classifications",
            "k": 5,
            "threshold": 0.1
        })
    }
    
    #[test]
    fn test_new_with_minimal_config() {
        let config = create_basic_config("ft_classifiers/lid176.bin");
        let result = FastTextAnnotator::new(&config);
        
        assert!(result.is_ok());
        let annotator = result.unwrap();
        
        assert_eq!(annotator.fast_text_file, "ft_classifiers/lid176.bin");
        assert_eq!(annotator.text_field, "text"); // Default value
        assert_eq!(annotator.output_field, "metadata.fasttext"); // Default value
        assert_eq!(annotator.k, 10); // Default value
        assert_eq!(annotator.threshold, 0.0); // Default value
    }
    
    #[test]
    fn test_new_with_custom_config() {
        let config = create_full_config("ft_classifiers/lid176.bin");
        let result = FastTextAnnotator::new(&config);
        
        assert!(result.is_ok());
        let annotator = result.unwrap();
        
        assert_eq!(annotator.fast_text_file, "ft_classifiers/lid176.bin");
        assert_eq!(annotator.text_field, "content.body");
        assert_eq!(annotator.output_field, "metadata.classifications");
        assert_eq!(annotator.k, 5);
        assert_eq!(annotator.threshold, 0.1);
    }
    
    
    #[test]
    fn test_process_simple_document() {
        let config = create_basic_config("ft_classifiers/lid176.bin");
        let annotator = FastTextAnnotator::new(&config).unwrap();
        
        let data = json!({
            "text": "This is a sample text document for classification",
            "metadata": {}
        });
        
        let result = annotator.process(data);
        assert!(result.is_ok());
        
        let processed = result.unwrap().unwrap();
        
        // Verify that the fasttext field was added
        assert!(processed.pointer("/metadata/fasttext").is_some());
        
        // The actual predictions will depend on the model, but we can
        // verify the structure is correct
        let predictions = processed.pointer("/metadata/fasttext").unwrap();
        assert!(predictions.is_object());
    }
    
    #[test]
    fn test_process_custom_fields() {
        let config = create_full_config("ft_classifiers/lid176.bin");
        let annotator = FastTextAnnotator::new(&config).unwrap();
        
        let data = json!({
            "content": {
                "body": "Another example text for classification"
            },
            "metadata": {}
        });
        
        let result = annotator.process(data);
        assert!(result.is_ok());
        
        let processed = result.unwrap().unwrap();
        
        // Verify that the custom field was added
        assert!(processed.pointer("/metadata/classifications").is_some());
        
        // Check that we got at most k predictions
        let predictions = processed.pointer("/metadata/classifications").unwrap().as_object().unwrap();
        assert!(predictions.len() <= 5); // k=5 in our config
        
        // Check all probabilities are above threshold
        for (_, prob) in predictions {
            assert!(prob.as_f64().unwrap() >= 0.1); // threshold=0.1
        }
    }
    
    
    #[test]
    fn test_process_empty_text() {
        let config = create_basic_config("ft_classifiers/lid176.bin");
        let annotator = FastTextAnnotator::new(&config).unwrap();
        
        let data = json!({
            "text": "",
            "metadata": {}
        });
        
        let result = annotator.process(data);
        assert!(result.is_ok());
        
        let processed = result.unwrap().unwrap();
        
        // The model might still return some predictions for empty text,
        // but we should have a valid JSON object
        assert!(processed.pointer("/metadata/fasttext").is_some());
        assert!(processed.pointer("/metadata/fasttext").unwrap().is_object());
    }
    
    #[test]
    fn test_process_nested_output_field() {
        let config = json!({
            "fast_text_file": "ft_classifiers/lid176.bin",
            "text_field": "text",
            "output_field": "deep.nested.classifications.fasttext",
            "k": 3,
            "threshold": 0.2
        });
        
        let annotator = FastTextAnnotator::new(&config).unwrap();
        
        let data = json!({
            "text": "Text for testing deeply nested output fields",
            "deep": {
                "nested": {
                    "classifications": {}
                }
            }
        });
        
        let result = annotator.process(data);
        assert!(result.is_ok());
        
        let processed = result.unwrap().unwrap();
        // Verify that the deeply nested field was added correctly
        assert!(processed.pointer("/deep/nested/classifications/fasttext").is_some());
        
        // Check that we got at most k predictions
        let predictions = processed.pointer("/deep/nested/classifications/fasttext").unwrap().as_object().unwrap();

        assert!(predictions.len() <= 3); // k=3 in this config
    }
    
    #[test]
    fn test_integration() {
        // This test simulates a realistic pipeline with multiple documents
        let config = create_basic_config("ft_classifiers/lid176.bin");
        let annotator = FastTextAnnotator::new(&config).unwrap();
        
        let documents = vec![
            json!({
                "id": "doc1",
                "text": "Short text about sports and games",
                "metadata": {}
            }),
            json!({
                "id": "doc2",
                "text": "A lengthy article discussing politics and economics in detail",
                "metadata": {}
            }),
            json!({
                "id": "doc3",
                "text": "Scientific research on climate change and global warming",
                "metadata": {}
            })
        ];
        
        for doc in documents {
            let result = annotator.process(doc.clone());
            assert!(result.is_ok());
            
            let processed = result.unwrap().unwrap();
            
            // Verify the original fields are preserved
            assert_eq!(processed["id"], doc["id"]);
            assert_eq!(processed["text"], doc["text"]);
            
            // Verify predictions were added
            assert!(processed.pointer("/metadata/fasttext").is_some());
            assert!(processed.pointer("/metadata/fasttext").unwrap().is_object());
        }
    }
}