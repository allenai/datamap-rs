# Map Command Documentation

## Overview

The Map command processes data through a highly customizable pipeline of filters, modifiers, and annotators. Each processor in the pipeline operates independently on individual documents, enabling embarrassingly parallel processing across your dataset.

## Usage
```bash
datamap map \
  --input_dir ./data/input \
  --output_dir ./data/output \
  --config pipeline_config.yaml \
  [--err_dir ./data/errors] \
  [--delete_after_read] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_dir`: Directory for processed output files
- `--config`: Path to YAML or JSON configuration file defining the pipeline
- `--err_dir`: (Optional) Directory to store documents that failed processing
- `--delete_after_read`: (Optional) Delete input files after successful processing
- `--threads`: (Optional) Number of threads to use (default: all available cores)

## Input/Output Format

### Input
- JSONL files (`.jsonl`, `.jsonl.gz`, `.jsonl.zst`, etc.)
- Each line is a valid JSON object
- Must contain fields referenced by your pipeline processors (typically includes a `text` field)

### Output Structure

The map command creates a structured output showing where documents were filtered at each pipeline step:
```
output_dir/
├── step_00/          # Documents filtered at step 0
│   └── file.jsonl
├── step_01/          # Documents filtered at step 1
│   └── file.jsonl
├── step_02/          # Documents filtered at step 2
│   └── file.jsonl
└── step_final/       # Documents that passed all filters
    └── file.jsonl
```

- `step_XX/`: Contains documents that were **filtered out** at step XX (corresponding to pipeline position)
- `step_final/`: Contains documents that **survived** the entire pipeline
- Each output file maintains the same name as its input file
- Documents that fail to parse as JSON are written to `err_dir` if specified

### Statistics Output

After processing, the command prints comprehensive statistics:
- Total processing time
- Number of documents processed
- For each pipeline step:
  - Percentage of processing time spent
  - Number of documents removed
  - Percentage of remaining documents removed
  - Percentage of total pool removed

## Configuration

Pipelines are defined using YAML or JSON configuration files:
```yaml
text_field: "text"  # Optional: specify which field contains text (defaults to "text")
pipeline:
  - name: "text_len_filter"
    kwargs:
      lower_bound: 100
      upper_bound: 100000
  - name: "subsample"
    kwargs:
      subsample_rate: 0.8
  - name: "stop_word_filter"
    kwargs:
      min_stop_word: 3
  - name: "word_count_adder"
    kwargs:
      word_count_field: "word_count"
```

## Available Processors

### Filters

Filters return `None` to remove documents from the pipeline.

#### Basic Filters
- **non_null_filter**: Removes documents that are JSON null values
- **text_len_filter**: Filters by character count in text field (lower_bound, upper_bound)
- **page_len_filter**: Filters by document length measured in words, sentences, lines, paragraphs, or characters
- **word_len_filter**: Filters by average word length
- **subsample**: Randomly samples documents at specified rate
- **float_filter**: Filters by numeric field values with optional range negation
- **string_eq_filter**: Filters by exact string field equality

#### Content Quality Filters
- **symbol_ratio_filter**: Filters by ratio of symbols (#, ..., ellipsis) to words
- **bullet_filter**: Filters by density of lines starting with bullet points
- **ellipsis_line_ratio_filter**: Filters by fraction of lines ending with ellipsis
- **alphabetic_word_ratio_filter**: Filters by ratio of non-alphabetic words
- **stop_word_filter**: Filters by presence of common English stop words
- **word_removal_ratio_filter**: Filters documents that lost too many words during processing (requires prior word count annotation)

#### Advanced Filters
- **url_substring_filter**: Comprehensive URL filtering with domain/subdomain matching, banlist support, and various matching modes (exact domain, subdomain, substring, etc.)
- **massive_web_repetition_filter**: Advanced repetition detection using rolling hash algorithm (based on Gopher paper methodology)
- **santcoder_pl_filter**: Filters for specific programming languages (Python, Java, Javascript)
- **madlad400_sentence_annotator**: Multi-criteria sentence-level quality analysis (document consistency, list case, abnormal lengths, technical characters, cursed patterns)
- **madlad400_rule_filter**: Filters based on Madlad400 sentence analysis annotations
- **interval_filter**: Removes text in specified character intervals with optional fuzzy interval merging

### Modifiers

Modifiers transform documents and return the modified version.

#### Content Modification
- **newline_removal_modifier**: Controls maximum consecutive newlines
- **ratio_line_modifier**: Removes lines with too many uppercase characters or digits
- **regex_line_modifier**: Removes lines matching regex patterns
- **line_len_modifier**: Removes lines below minimum word count
- **substring_line_modifier**: Removes lines containing banned substrings or removes just the substrings

#### Data Enrichment
- **add_id**: Adds UUID4 identifier to documents
- **word_count_adder**: Adds word count field (useful for tracking changes through pipeline)
- **hash_annotator**: Adds hash of specified field (64-bit or 128-bit xxHash)
- **constant_annotator**: Adds constant string value to all documents
- **rename_modifier**: Renames fields in JSON documents

### Annotators

Annotators add metadata without filtering.

- **fasttext_annotator**: Adds language/topic classification using FastText models (top-k predictions with probability threshold)
- **madlad400_sentence_annotator**: Detailed sentence-level quality analysis with rule-based annotations
- **dd_max_getter**: Extracts key with maximum value from attributes with specified prefix
- **max_extractor**: Extracts key with maximum value from a dictionary field

## Performance Characteristics

- **Parallel Processing**: Each input file is processed independently across multiple threads
- **Sequential Pipeline**: Within each file, documents flow sequentially through pipeline steps
- **Memory Efficiency**: Files are processed one at a time; memory scales with individual file size and thread count
- **Early Exit**: Documents filtered at any step stop processing immediately

## Examples

### Basic Quality Filtering
```yaml
text_field: "text"
pipeline:
  - name: "text_len_filter"
    kwargs:
      lower_bound: 100
      upper_bound: 50000
  - name: "stop_word_filter"
    kwargs:
      min_stop_word: 5
```

### Deduplication Pipeline
```yaml
pipeline:
  - name: "word_count_adder"
    kwargs:
      word_count_field: "original_word_count"
  - name: "massive_web_repetition_filter"
  - name: "word_removal_ratio_filter"
    kwargs:
      word_count_field: "original_word_count"
      upper_bound: 0.3
```

### Language Classification
```yaml
pipeline:
  - name: "fasttext_annotator"
    kwargs:
      fast_text_file: "./models/lid176.bin"
      output_field: "metadata.language"
      k: 3
      threshold: 0.5
  - name: "string_eq_filter"
    kwargs:
      str_field: "metadata.language.__label__en"
      eq: "__label__en"
```