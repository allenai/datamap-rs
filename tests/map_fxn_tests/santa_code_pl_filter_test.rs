// extern crate datamap_rs;
// use serde_json::json;
// //use datamap_rs::{DataProcessor};
// use datamap_rs::map_fxn::{DataProcessor, SantaCoderPLFilter};




// #[test]
// fn test_santacoder_pl_filter_new() {
//     // Test with default configuration
//     let config = json!({});
//     let filter = SantaCoderPLFilter::new(&config).unwrap();
//     assert_eq!(filter.pl_key, "metadata.language");

//     // Test with custom pl_key
//     let config = json!({"pl_key": "language"});
//     let filter = SantaCoderPLFilter::new(&config).unwrap();
//     assert_eq!(filter.pl_key, "language");
// }

// #[test]
// fn test_santacoder_pl_filter_process_python() {
//     let filter = SantaCoderPLFilter {
//         pl_key: String::from("metadata.language"),
//     };

//     let data = json!({
//         "metadata": {
//             "language": "Python"
//         },
//         "content": "def hello_world():\n    print('Hello, World!')"
//     });

//     let result = filter.process(data.clone()).unwrap();
//     assert_eq!(result, Some(data));
// }

// #[test]
// fn test_santacoder_pl_filter_process_java() {
//     let filter = SantaCoderPLFilter {
//         pl_key: String::from("metadata.language"),
//     };

//     let data = json!({
//         "metadata": {
//             "language": "Java"
//         },
//         "content": "public class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(\"Hello, World!\");\n    }\n}"
//     });

//     let result = filter.process(data.clone()).unwrap();
//     assert_eq!(result, Some(data));
// }

// #[test]
// fn test_santacoder_pl_filter_process_javascript() {
//     let filter = SantaCoderPLFilter {
//         pl_key: String::from("metadata.language"),
//     };

//     let data = json!({
//         "metadata": {
//             "language": "Javascript"
//         },
//         "content": "console.log('Hello, World!');"
//     });

//     let result = filter.process(data.clone()).unwrap();
//     assert_eq!(result, Some(data));
// }

// #[test]
// fn test_santacoder_pl_filter_process_other_language() {
//     let filter = SantaCoderPLFilter {
//         pl_key: String::from("metadata.language"),
//     };

//     let data = json!({
//         "metadata": {
//             "language": "Rust"
//         },
//         "content": "fn main() {\n    println!(\"Hello, World!\");\n}"
//     });

//     let result = filter.process(data).unwrap();
//     assert_eq!(result, None);
// }

// #[test]
// fn test_santacoder_pl_filter_custom_path() {
//     let filter = SantaCoderPLFilter {
//         pl_key: String::from("lang"),
//     };

//     let python_data = json!({
//         "lang": "Python",
//         "content": "print('Hello')"
//     });

//     let rust_data = json!({
//         "lang": "Rust",
//         "content": "println!(\"Hello\");"
//     });

//     assert_eq!(filter.process(python_data.clone()).unwrap(), Some(python_data));
//     assert_eq!(filter.process(rust_data).unwrap(), None);
// }

// #[test]
// fn test_santacoder_pl_filter_case_sensitivity() {
//     let filter = SantaCoderPLFilter {
//         pl_key: String::from("metadata.language"),
//     };

//     // Test with lowercase "python" which should be filtered out
//     let data = json!({
//         "metadata": {
//             "language": "python"
//         },
//         "content": "def hello_world():\n    print('Hello, World!')"
//     });

//     let result = filter.process(data).unwrap();
//     assert_eq!(result, None);
// }
