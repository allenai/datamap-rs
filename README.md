# DataMap

A high-performance data processing pipeline for large-scale text datasets built in Rust.

## Overview

DataMap is a Rust-based toolkit designed for efficient processing, filtering, and resharding of large text datasets, primarily in JSONL format. It provides a flexible pipeline architecture for text data transformations with various filters and modifiers.

Key features:
- Multi-threaded processing with Rayon
- Configurable processing pipeline via JSON/YAML configuration
- Comprehensive set of text filters and modifiers
- Data resharding and partitioning capabilities
- High-performance parallel file processing

## Commands

DataMap provides three main subcommands:

### Map
Process data through a filtering/modification pipeline:

```bash
datamap map --input_dir ./data/input --output_dir ./data/output --config pipeline_config.yaml [--err_dir ./data/errors] [--threads 16]
```

### Reshard
Reshard files into specific size or line count chunks:

```bash
datamap reshard --input_dir ./data/input --output_dir ./data/output [--max_lines 10000] [--max_size 100000000] [--subsample 0.1] [--keep_dirs] [--delete_after_read] [--threads 16]
```

### Partition
Partition data based on configuration:

```bash
datamap partition --input_dir ./data/input --output_dir ./data/output --config partition_config.yaml [--threads 16]
```

## Configuration

Pipelines are defined using YAML or JSON configuration files. The configuration specifies a sequence of processors to apply:

```yaml
text_field: "text"  # Optional: specify which field contains the text (defaults to "text")
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

**Basic Filters:**
- `non_null_filter`: Remove null documents
- `text_len_filter`: Filter by text length (character count)
- `page_len_filter`: Filter by document length (words, sentences, lines, paragraphs, or characters)
- `word_len_filter`: Filter by average word length
- `subsample`: Randomly subsample documents
- `float_filter`: Filter by numeric field values with optional negation
- `string_eq_filter`: Filter by string field equality

**Content Quality Filters:**
- `symbol_ratio_filter`: Filter by symbol-to-word ratio
- `bullet_filter`: Filter by bullet point density
- `ellipsis_line_ratio_filter`: Filter by lines ending with ellipsis
- `alphabetic_word_ratio_filter`: Filter by ratio of non-alphabetic words
- `stop_word_filter`: Filter by presence of common stop words
- `word_removal_ratio_filter`: Filter documents that lost too many words during processing

**Advanced Filters:**
- `url_substring_filter`: Comprehensive URL filtering with domain/subdomain matching, banlist support, and various matching modes
- `massive_web_repetition_filter`: Advanced repetition detection using rolling hash (based on Gopher paper)
- `santcoder_pl_filter`: Filter for specific programming languages (Python, Java, Javascript)
- `madlad400_sentence_annotator`: Multi-criteria sentence analysis from Madlad400 paper
- `madlad400_rule_filter`: Filter based on Madlad400 sentence analysis results
- `interval_filter`: Filter text based on character intervals with optional fuzzy merging

### Modifiers

**Content Modification:**
- `newline_removal_modifier`: Control maximum consecutive newlines
- `ratio_line_modifier`: Remove lines with too many uppercase chars or digits
- `regex_line_modifier`: Remove lines matching regex patterns
- `line_len_modifier`: Remove lines below minimum word count
- `substring_line_modifier`: Remove lines containing banned substrings or just remove the substrings

**Data Enrichment:**
- `add_id`: Add UUID4 to documents
- `word_count_adder`: Add word count field
- `hash_annotator`: Add hash of specified field (64-bit or 128-bit xxHash)
- `constant_annotator`: Add constant string value to all documents
- `rename_modifier`: Rename fields in JSON

### Annotators

**Language and Classification:**
- `fasttext_annotator`: Add language/topic classification using FastText models
- `madlad400_sentence_annotator`: Detailed sentence-level quality analysis

**Data Extraction:**
- `dd_max_getter`: Extract maximum value from attributes with specified prefix
- `max_extractor`: Extract key with maximum value from a dictionary field

## Architecture

### Pipeline Processing

The system uses a trait-based architecture where each processor implements the `DataProcessor` trait:

```rust
pub trait DataProcessor {
    fn new(config: &Value) -> Result<Self, Error> where Self: Sized;
    fn process(&self, data: Value) -> Result<Option<Value>, Error>;
}
```

Processors can:
- Return `Some(modified_data)` to pass the document to the next stage
- Return `None` to filter out the document
- Return an `Error` for processing failures

### Performance Features

- **Parallel Processing**: Uses Rayon for multi-threaded file processing
- **Efficient String Operations**: Optimized text processing with minimal allocations
- **Memory Management**: Streaming processing to handle large datasets
- **Comprehensive Logging**: Detailed timing and filtering statistics

### Output Organization

The map command can output intermediate results from each pipeline step:
- `step_00/`, `step_01/`, etc. for each pipeline stage
- `step_final/` for documents that pass all filters
- Optional error directory for documents that failed processing

## Dependencies

### Core Dependencies
- `rayon` - Parallel processing
- `clap` - Command-line interface
- `serde_json`/`serde_yaml` - Configuration parsing
- `anyhow` - Error handling
- `dashmap` - Concurrent data structures
- `mj_io` - custom io for interacting with the local file system

### Text Processing
- `regex` - Pattern matching
- `unicode_segmentation` - Unicode-aware text processing
- `aho_corasick` - Efficient string matching
- `url` - URL parsing

### Specialized
- `fasttext` - Language classification
- `xxhash_rust` - Fast hashing
- `uuid` - Unique ID generation

### Python Utilities (`utils/s5cmd_wrapper.py`)

Python utilities for cloud storage operations:
- S3/GCP/WEKA integration via s5cmd
- Parallel file download/upload capabilities
- Progress tracking

### Cloud Storage Integration

Upload/download files from cloud storage:

```bash
python utils/s5cmd_wrapper.py download --src s3://bucket/path --dst ./local/path [--part 0 --num-parts 4]
python utils/s5cmd_wrapper.py upload --src ./local/path --dst s3://bucket/path
```

## Installation

1. Install Rust: https://www.rust-lang.org/tools/install
2. Clone the repository
3. Build the project:
   ```bash
   cargo build --release
   ```
4. Install Python dependencies (for cloud utilities):
   ```bash
   pip install boto3 click tqdm
   ```
5. Install s5cmd if using cloud storage utilities:
   ```bash
   # Instructions vary by platform
   ```

## Performance Notes

- The pipeline processes files in parallel using all available CPU cores by default
- Use `--threads N` to limit parallelism
- Documents are processed sequentially through the pipeline stages
- Memory usage scales with the number of parallel files being processed
- Large documents may require additional memory for text processing operations

## License

[Insert your license information here]