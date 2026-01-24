extern crate datamap_rs;
use datamap_rs::map_fxn::{convert_spaces_to_tabs, detect_indentation};

// ============================================================================
// detect_indentation tests
// ============================================================================

#[test]
fn test_detect_indentation_4_spaces() {
    let text = "def foo():\n    if True:\n        print('hello')\n    return";
    let result = detect_indentation(text);
    assert_eq!(result, Some(4));
}

#[test]
fn test_detect_indentation_2_spaces() {
    let text = "function bar() {\n  let x = 1;\n  if (x) {\n    return x;\n  }\n}";
    let result = detect_indentation(text);
    assert_eq!(result, Some(2));
}

#[test]
fn test_detect_indentation_no_indentation() {
    let text = "line one\nline two\nline three";
    let result = detect_indentation(text);
    assert_eq!(result, None);
}

// ============================================================================
// convert_spaces_to_tabs tests
// ============================================================================

#[test]
fn test_convert_spaces_to_tabs_4_spaces() {
    let text = "def foo():\n    if True:\n        print('hello')";
    let result = convert_spaces_to_tabs(text, None);
    assert_eq!(result, "def foo():\n\tif True:\n\t\tprint('hello')");
}

#[test]
fn test_convert_spaces_to_tabs_2_spaces() {
    let text = "function bar() {\n  let x = 1;\n    nested;\n}";
    let result = convert_spaces_to_tabs(text, None);
    assert_eq!(result, "function bar() {\n\tlet x = 1;\n\t\tnested;\n}");
}

#[test]
fn test_convert_spaces_to_tabs_preserves_non_indented() {
    let text = "no indentation here\njust plain text\nnothing special";
    let result = convert_spaces_to_tabs(text, None);
    assert_eq!(result, text);
}
