# Count Command Documentation

## Overview

The Count command computes statistics about a dataset, including the number of documents, total uncompressed file size, and optionally the total size of a specific text field across all documents. This is useful for dataset validation, understanding data volume, and tracking changes through processing pipelines.

## Usage
```bash
datamap count \
  --input_dir ./data/input \
  --output_file ./stats/counts.json \
  [--count_bytes "text"] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_file`: Path to output JSON file for statistics
- `--count_bytes`: (Optional) JSON field to count total byte size (e.g., "text", "metadata.content")
- `--threads`: (Optional) Number of threads to use (default: all available cores)

## Input/Output Format

### Input
- JSONL files in any format (`.jsonl`, `.jsonl.gz`, `.jsonl.zst`, etc.)
- Any valid JSONL documents

### Output Format

The command writes a JSON file with the following structure:
```json
{
  "total_docs": 1234567,
  "total_file_size": 10737418240,
  "total_text_bytes": 8589934592
}
```

**Fields:**
- `total_docs`: Total number of JSONL documents across all files
- `total_file_size`: Total uncompressed size in bytes of all JSONL data
- `total_text_bytes`: Total bytes in the specified field (0 if `--count_bytes` not provided)

### Console Output

The command also prints a summary to stdout:
```
Saw 1234567 docs (10737418240 bytes)| 8589934592 text bytes | in 45 secs
```

## How It Works

1. **Parallel Processing**: All input files are processed simultaneously across threads
2. **Line Counting**: Counts each line as one document
3. **Size Calculation**: Sums the uncompressed byte size of each line
4. **Field Extraction**: If `--count_bytes` specified, extracts and measures the specified field
5. **Aggregation**: Atomic counters aggregate statistics across threads
6. **Output**: Writes JSON results to file and prints summary

## Performance Characteristics

- **Speed**: Fast, single-pass streaming operation
- **Memory Usage**: Very low, processes files line-by-line
- **Parallelism**: High, all files processed in parallel
- **I/O Bound**: Performance limited by disk read throughput


## Examples

### Simple Count
```bash
datamap count \
  --input_dir ./dataset \
  --output_file ./counts.json
```

**Output (counts.json):**
```json
{
  "total_docs": 1000000,
  "total_file_size": 5368709120,
  "total_text_bytes": 0
}
```

### Count Text Field
```bash
datamap count \
  --input_dir ./dataset \
  --output_file ./counts.json \
  --count_bytes "text"
```

**Output (counts.json):**
```json
{
  "total_docs": 1000000,
  "total_file_size": 5368709120,
  "total_text_bytes": 4294967296
}
```


## Performance Notes

- **Very Fast**: Optimized for speed with parallel processing
- **Low Memory**: Streaming operation with minimal memory footprint
- **Accurate**: Counts actual uncompressed sizes regardless of file compression
- **Progress Bar**: Shows progress as files are processed

## Limitations

- **Line-Based Counting**: Assumes each line is one document (doesn't parse JSON)
- **No Validation**: Doesn't verify JSON validity, only counts lines
- **Field Extraction**: Only counts the first level of the specified field path
- **No Schema Analysis**: Doesn't report field types or structure

## Notes

- Safe to run on large datasets (terabytes+)
- Results are deterministic (same input always produces same output)
- Can be used as a checkpoint in data pipelines
- Useful for debugging unexpected data loss or gain
- Console output includes timing information