# DataMap

A high-performance data processing pipeline for large-scale text datasets built in Rust.

## Table of Contents

- [Overview](#overview)
- [Commands](#commands)
  - [Map](#map)
  - [Reshard](#reshard)
  - [Partition](#partition)
- [Configuration](#configuration)
- [Available Processors](#available-processors)
  - [Filters](#filters)
  - [Modifiers](#modifiers)
  - [Annotators](#annotators)
- [Architecture](#architecture)
  - [Pipeline Processing](#pipeline-processing)
  - [Performance Features](#performance-features)
  - [Output Organization](#output-organization)
- [Cloud Storage Integration](#cloud-storage-integration)
- [Installation](#installation)
- [Performance Notes](#performance-notes)
- [Examples](#examples)
- [License](#license)

## Overview

DataMap is a Rust-based toolkit designed for efficient processing, filtering, and resharding of large text datasets, primarily in JSONL format. It provides a flexible pipeline architecture for text data transformations with various filters and modifiers.

Key features:
- Multi-threaded processing with Rayon
- Configurable processing pipeline via JSON/YAML configuration
- Comprehensive set of text filters and modifiers
- Data resharding and partitioning capabilities
- High-performance parallel file processing

NOTE: This is intended to be used on local files only. This is because of the unstable nature of rust cloud storage wrappers. We highly recommend heavy utilization of [i4i ec2 instances](https://aws.amazon.com/ec2/instance-types/i4i/) which can be equipped with large AWS Nitro Drives.

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

## Cloud Storage Integration
We strongly recommend using [s5cmd](https://github.com/peak/s5cmd) for efficient interaction with s3. We find that the workflow of downloading objects to local storage via s5cmd, processing them, and then reuploading the processed data is more efficient and stable than trying to interact with s3 data directly.

You can use the s5cmd CLI directly or the python wrappers:

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
   # For linux systems: 
   wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz 
   tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz 
   sudo mv s5cmd /usr/local/bin
   ```

## Performance Notes

- The pipeline processes files in parallel using all available CPU cores by default
- Use `--threads N` to limit parallelism
- Documents are processed sequentially through the pipeline stages
- Memory usage scales with the number of parallel files being processed
- Large documents may require additional memory for text processing operations

## Examples 
As examples, we provide configuration files for the DCLM data processing flow as well as our All-Dressed™ mixture. These are almost entirely plug-and-play, with the exception of requiring a download of the lid176.bin language classification fasttext model. To do this, execute the download script at `/configs/all_dressed/download_lid.sh`

## License

[Insert your license information here]