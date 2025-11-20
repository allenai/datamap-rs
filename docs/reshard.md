# Reshard Command Documentation

## Overview

The Reshard command takes a data pool with files of uneven sizes and reorganizes them into uniformly-sized shards. This is particularly useful for creating optimally-sized files (typically ~256MB) that balance parallel processing efficiency with memory usage. The command can optionally respect subdirectory structure during resharding.

## Usage
```bash
datamap reshard \
  --input_dir ./data/input \
  --output_dir ./data/output \
  [--max_lines 10000] \
  [--max_size 256000000] \
  [--subsample 0.1] \
  [--keep_dirs] \
  [--delete_after_read] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_dir`: Directory for resharded output files
- `--max_lines`: (Optional) Maximum number of lines per output shard (default: unlimited)
- `--max_size`: (Optional) Maximum size in bytes per output shard (default: unlimited, recommended: 256000000 for 256MB)
- `--subsample`: (Optional) Subsample rate (0.0-1.0) to randomly sample documents (default: 0.0 = no sampling)
- `--keep_dirs`: (Optional) Preserve subdirectory structure from input
- `--delete_after_read`: (Optional) Delete input files after successful processing
- `--threads`: (Optional) Number of threads to use (default: all available cores)

**Note**: At least one of `--max_lines` or `--max_size` must be specified.

## Input/Output Format

### Input
- JSONL files in any format (`.jsonl`, `.jsonl.gz`, `.jsonl.zst`, etc.)
- Files can be of varying sizes
- Can be organized in subdirectories

### Output Structure

The reshard command creates uniformly-sized output shards:
```
output_dir/
├── shard_00000000.jsonl.zst
├── shard_00000001.jsonl.zst
├── shard_00000002.jsonl.zst
└── ...
```

**With `--keep_dirs` flag:**
```
output_dir/
├── subdir1/
│   ├── shard_00000000.jsonl.zst
│   ├── shard_00000001.jsonl.zst
│   └── ...
├── subdir2/
│   ├── shard_00000002.jsonl.zst
│   ├── shard_00000003.jsonl.zst
│   └── ...
└── ...
```

- Output files are always named `shard_########.jsonl.zst` with zero-padded IDs
- Output files are compressed with zstd (level 3 compression)
- All output shards respect the specified size/line limits

## How It Works

1. **Grouping**: Files are grouped by thread or by directory (if `--keep_dirs` is used)
2. **Streaming**: Documents are streamed from input files into new output shards
3. **Splitting**: When a shard reaches `max_lines` or `max_size`, a new shard is created
4. **Balancing**: Work is distributed across threads for parallel processing

### Size Control

- `--max_size`: Controls uncompressed byte size (e.g., 268435456 = 256MB)
- `--max_lines`: Controls number of JSONL documents
- Whichever limit is reached first triggers creation of a new shard

### Directory Preservation

With `--keep_dirs`:
- Input directory structure is preserved in output
- Files within each subdirectory are processed together
- Ensures related documents remain in the same output directory

## Performance Characteristics

- **Parallel Processing**: Multiple subdirectories or file groups are processed simultaneously
- **Streaming**: Low memory footprint as files are processed line-by-line
- **Compression**: Output files are zstd-compressed on-the-fly
- **Thread Balancing**: Large subdirectories are automatically split across threads

## Common Use Cases

### Create 256MB Shards
```bash
datamap reshard \
  --input_dir ./raw_data \
  --output_dir ./sharded_data \
  --max_size 256000000
```

### Reshard While Subsampling
```bash
datamap reshard \
  --input_dir ./full_dataset \
  --output_dir ./sampled_dataset \
  --max_size 256000000 \
  --subsample 0.1
```

### Preserve Directory Structure
```bash
datamap reshard \
  --input_dir ./data/by_source \
  --output_dir ./data/sharded_by_source \
  --max_size 256000000 \
  --keep_dirs
```

### Create Fixed-Size Training Shards
```bash
datamap reshard \
  --input_dir ./processed \
  --output_dir ./training_shards \
  --max_lines 50000 \
  --delete_after_read
```

## Why 256MB?

The recommended 256MB shard size (256000000 bytes) represents a "sweet spot" for most use cases:
- Large enough to minimize file system overhead
- Small enough to fit comfortably in memory during processing
- Optimal for parallel processing across many files
- Good balance for network transfer and caching

## Output Statistics

After completion, the command prints:
- Total processing time
- Number of output shards created

## Notes

- Output shards do not preserve original file boundaries
- Documents from multiple input files may be combined into single output shards
- Compression is applied automatically (zstd level 3)
- If both `max_lines` and `max_size` are specified, the first limit reached triggers a new shard