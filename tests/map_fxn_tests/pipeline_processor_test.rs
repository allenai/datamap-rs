extern crate datamap_rs;
use datamap_rs::map_fxn::PipelineProcessor;
use serde_json::json;
use std::path::PathBuf;

#[test]
fn test_default_step_names_single_step() {
    // A single-step pipeline should have the step named "step_final"
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 1);
    assert_eq!(processor.steps[0], "step_final");
}

#[test]
fn test_default_step_names_two_steps() {
    // Two steps: first is step_00, second (last) is step_final
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}},
            {"name": "non_null_filter", "kwargs": {}}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 2);
    assert_eq!(processor.steps[0], "step_00");
    assert_eq!(processor.steps[1], "step_final");
}

#[test]
fn test_default_step_names_multiple_steps() {
    // Multiple steps: step_00, step_01, ..., step_final
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "text_len_filter", "kwargs": {"upper_bound": 1000}},
            {"name": "non_null_filter", "kwargs": {}}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 4);
    assert_eq!(processor.steps[0], "step_00");
    assert_eq!(processor.steps[1], "step_01");
    assert_eq!(processor.steps[2], "step_02");
    assert_eq!(processor.steps[3], "step_final");
}

#[test]
fn test_custom_step_name_single_step() {
    // Custom step name for a single-step pipeline
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}, "step": "my_custom_step"}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 1);
    assert_eq!(processor.steps[0], "my_custom_step");
}

#[test]
fn test_custom_step_names_all_steps() {
    // All steps have custom names
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}, "step": "length_filter"},
            {"name": "non_null_filter", "kwargs": {}, "step": "null_check"},
            {"name": "text_len_filter", "kwargs": {"upper_bound": 1000}, "step": "max_length"}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 3);
    assert_eq!(processor.steps[0], "length_filter");
    assert_eq!(processor.steps[1], "null_check");
    assert_eq!(processor.steps[2], "max_length");
}

#[test]
fn test_mixed_custom_and_default_step_names() {
    // Some steps have custom names, others use defaults
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}, "step": "first_filter"},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "text_len_filter", "kwargs": {"upper_bound": 1000}, "step": "third_filter"},
            {"name": "non_null_filter", "kwargs": {}}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 4);
    assert_eq!(processor.steps[0], "first_filter");
    assert_eq!(processor.steps[1], "step_01");
    assert_eq!(processor.steps[2], "third_filter");
    assert_eq!(processor.steps[3], "step_final");
}

#[test]
fn test_custom_step_name_on_last_step_overrides_final() {
    // Custom name on last step should override "step_final"
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 5}},
            {"name": "non_null_filter", "kwargs": {}, "step": "custom_final"}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 2);
    assert_eq!(processor.steps[0], "step_00");
    assert_eq!(processor.steps[1], "custom_final");
}

#[test]
fn test_step_names_with_twelve_steps() {
    // Test the example from the user: 12 steps should be step_00 through step_10, then step_final
    let config = json!({
        "pipeline": [
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}},
            {"name": "non_null_filter", "kwargs": {}}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 12);
    assert_eq!(processor.steps[0], "step_00");
    assert_eq!(processor.steps[1], "step_01");
    assert_eq!(processor.steps[2], "step_02");
    assert_eq!(processor.steps[3], "step_03");
    assert_eq!(processor.steps[4], "step_04");
    assert_eq!(processor.steps[5], "step_05");
    assert_eq!(processor.steps[6], "step_06");
    assert_eq!(processor.steps[7], "step_07");
    assert_eq!(processor.steps[8], "step_08");
    assert_eq!(processor.steps[9], "step_09");
    assert_eq!(processor.steps[10], "step_10");
    assert_eq!(processor.steps[11], "step_final");
}

#[test]
fn test_process_lines_returns_correct_step_indices() {
    // Verify that process_lines groups filtered documents by step index
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 10}},
            {"name": "text_len_filter", "kwargs": {"upper_bound": 50}}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    // Lines that will be filtered at different stages:
    // - "short" (5 chars): filtered at step 0 (lower_bound 10)
    // - "this is a medium length text" (28 chars): passes both filters
    // - "this is a very very very very very very very very long text that exceeds fifty characters" (90 chars): filtered at step 1 (upper_bound 50)
    let lines = vec![
        r#"{"text": "short"}"#.to_string(),
        r#"{"text": "this is a medium length text"}"#.to_string(),
        r#"{"text": "this is a very very very very very very very very long text that exceeds fifty characters"}"#.to_string(),
    ];

    let filename = PathBuf::from("test.jsonl");
    let (output_lines, err_lines, _timing, filter_info) =
        processor.process_lines(lines, &filename).unwrap();

    // Check that documents were grouped by their filter step
    assert!(err_lines.is_empty());

    // Step 0 should have 1 document (filtered by lower_bound)
    assert_eq!(output_lines.get(&0).map(|v| v.len()).unwrap_or(0), 1);

    // Step 1 should have 1 document (filtered by upper_bound)
    assert_eq!(output_lines.get(&1).map(|v| v.len()).unwrap_or(0), 1);

    // usize::MAX should have 1 document (passed all filters)
    assert_eq!(
        output_lines.get(&usize::MAX).map(|v| v.len()).unwrap_or(0),
        1
    );

    // Verify filter_info counts
    assert_eq!(*filter_info.get(&0).unwrap_or(&0), 1); // 1 filtered at step 0
    assert_eq!(*filter_info.get(&1).unwrap_or(&0), 1); // 1 filtered at step 1
    assert_eq!(*filter_info.get(&usize::MAX).unwrap_or(&0), 1); // 1 passed
}

#[test]
fn test_step_names_used_for_output_grouping() {
    // Verify that the step names correspond to the indices used in output_lines
    let config = json!({
        "pipeline": [
            {"name": "text_len_filter", "kwargs": {"lower_bound": 10}, "step": "min_length"},
            {"name": "text_len_filter", "kwargs": {"upper_bound": 50}, "step": "max_length"}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    // Verify step names
    assert_eq!(processor.steps[0], "min_length");
    assert_eq!(processor.steps[1], "max_length");

    // The indices in output_lines (0, 1, usize::MAX) can be used to look up
    // the corresponding step name in processor.steps
    let lines = vec![
        r#"{"text": "short"}"#.to_string(), // filtered at index 0 -> "min_length"
    ];

    let filename = PathBuf::from("test.jsonl");
    let (output_lines, _, _, _) = processor.process_lines(lines, &filename).unwrap();

    // Document filtered at step 0
    assert!(output_lines.contains_key(&0));
    // The step name for index 0 is "min_length"
    assert_eq!(processor.steps[0], "min_length");
}

#[test]
fn test_empty_pipeline() {
    // An empty pipeline should work (all documents pass through)
    let config = json!({
        "pipeline": []
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 0);
    assert_eq!(processor.pipeline.len(), 0);
}

#[test]
fn test_step_name_with_special_characters() {
    // Step names with special characters should be preserved
    let config = json!({
        "pipeline": [
            {"name": "non_null_filter", "kwargs": {}, "step": "step-with-dashes"},
            {"name": "non_null_filter", "kwargs": {}, "step": "step_with_underscores"},
            {"name": "non_null_filter", "kwargs": {}, "step": "step.with.dots"}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 3);
    assert_eq!(processor.steps[0], "step-with-dashes");
    assert_eq!(processor.steps[1], "step_with_underscores");
    assert_eq!(processor.steps[2], "step.with.dots");
}

#[test]
fn test_numeric_step_names() {
    // Numeric step names should work
    let config = json!({
        "pipeline": [
            {"name": "non_null_filter", "kwargs": {}, "step": "001"},
            {"name": "non_null_filter", "kwargs": {}, "step": "002"}
        ]
    });

    let processor = PipelineProcessor::new(&config).unwrap();

    assert_eq!(processor.steps.len(), 2);
    assert_eq!(processor.steps[0], "001");
    assert_eq!(processor.steps[1], "002");
}
