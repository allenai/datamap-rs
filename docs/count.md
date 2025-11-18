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

## Common Use Cases

### Basic Document Count
```bash
datamap count \
  --input_dir ./dataset \
  --output_file ./stats/counts.json
```

### Count with Text Field Size
```bash
datamap count \
  --input_dir ./dataset \
  --output_file ./stats/counts.json \
  --count_bytes "text"
```

### Validate Pipeline Output
```bash
# Before processing
datamap count \
  --input_dir ./data/raw \
  --output_file ./stats/before.json \
  --count_bytes "text"

# After processing
datamap count \
  --input_dir ./data/processed \
  --output_file ./stats/after.json \
  --count_bytes "text"

# Compare to verify data loss/gain
```

### Track Multiple Dataset Versions
```bash
# Original dataset
datamap count \
  --input_dir ./v1 \
  --output_file ./stats/v1_counts.json \
  --count_bytes "text"

# After filtering
datamap count \
  --input_dir ./v2_filtered \
  --output_file ./stats/v2_counts.json \
  --count_bytes "text"

# After deduplication
datamap count \
  --input_dir ./v3_deduped \
  --output_file ./stats/v3_counts.json \
  --count_bytes "text"
```

### Measure Field Sizes for Different Keys
```bash
# Count total text bytes
datamap count \
  --input_dir ./data \
  --output_file ./stats/text_bytes.json \
  --count_bytes "text"

# Count total summary bytes
datamap count \
  --input_dir ./data \
  --output_file ./stats/summary_bytes.json \
  --count_bytes "metadata.summary"
```

## Understanding the Output

### `total_docs`
- Number of lines in all JSONL files
- Each line counted as one document (even if JSON parsing would fail)
- Useful for comparing before/after filtering operations

### `total_file_size`
- Sum of uncompressed byte sizes of all lines
- Includes JSON formatting, whitespace, and newlines
- Represents the actual data volume regardless of compression

### `total_text_bytes`
- Sum of byte sizes of the specified field across all documents
- Only counts documents where the field exists
- Useful for estimating token counts or text volume
- Zero if `--count_bytes` not specified

## Field Extraction Details

When using `--count_bytes`:
- Uses `gjson` for fast field extraction
- Supports nested fields with dot notation (e.g., "metadata.text")
- If field doesn't exist in a document, that document contributes 0 bytes
- Only counts the string value (not JSON encoding overhead)
- Non-string fields are converted to strings for counting

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

### Count Nested Field
```bash
datamap count \
  --input_dir ./dataset \
  --output_file ./counts.json \
  --count_bytes "metadata.content.body"
```

## Integration with Other Commands

### Before/After Filtering
```bash
# Count before filtering
datamap count \
  --input_dir ./data/input \
  --output_file ./stats/before.json \
  --count_bytes "text"

# Filter data
datamap map \
  --input_dir ./data/input \
  --output_dir ./data/filtered \
  --config filter_config.yaml

# Count after filtering
datamap count \
  --input_dir ./data/filtered/step_final \
  --output_file ./stats/after.json \
  --count_bytes "text"
```

### Track Deduplication Impact
```bash
# Before deduplication
datamap count \
  --input_dir ./data/raw \
  --output_file ./stats/with_dupes.json

# After deduplication
datamap count \
  --input_dir ./data/deduped \
  --output_file ./stats/deduped.json

# Calculate duplicate rate: (before - after) / before
```

### Validate Reshard Operation
```bash
# Count before reshard
datamap count \
  --input_dir ./uneven_files \
  --output_file ./stats/before_reshard.json

# Reshard
datamap reshard \
  --input_dir ./uneven_files \
  --output_dir ./resharded \
  --max_size 268435456

# Count after reshard (should match)
datamap count \
  --input_dir ./resharded \
  --output_file ./stats/after_reshard.json
```

## Calculating Derived Statistics

### Average Document Size
```python
import json

with open('counts.json') as f:
    stats = json.load(f)
    
avg_size = stats['total_file_size'] / stats['total_docs']
print(f"Average document size: {avg_size:.2f} bytes")
```

### Text-to-JSON Ratio
```python
import json

with open('counts.json') as f:
    stats = json.load(f)
    
ratio = stats['total_text_bytes'] / stats['total_file_size']
print(f"Text comprises {ratio*100:.1f}% of total JSON size")
```

### Estimate Token Count
```python
import json

with open('counts.json') as f:
    stats = json.load(f)
    
# Rough estimate: 1 token ≈ 4 bytes
estimated_tokens = stats['total_text_bytes'] / 4
print(f"Estimated tokens: {estimated_tokens:,.0f}")
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