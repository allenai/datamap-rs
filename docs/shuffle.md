# Shuffle Command Documentation

## Overview

The Shuffle command coarsely redistributes data across a specified number of output files. Each document is randomly assigned to one of the output files, but the data within each output file is **not** shuffled internally. This provides a lightweight way to redistribute data across many files for parallel processing or to break up patterns in the original file organization.

## Usage
```bash
datamap shuffle \
  --input_dir ./data/input \
  --output_dir ./data/shuffled \
  --num_outputs 100 \
  [--max_len 268435456] \
  [--delete_after_read] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_dir`: Directory for shuffled output files
- `--num_outputs`: Number of output files to create (required)
- `--max_len`: (Optional) Maximum uncompressed bytes per output file (default: 268435456 = 256MB)
- `--delete_after_read`: (Optional) Delete input files after successful processing
- `--threads`: (Optional) Number of threads to use (default: all available cores)

## Input/Output Format

### Input
- JSONL files in any format (`.jsonl`, `.jsonl.gz`, `.jsonl.zst`, etc.)
- Any valid JSONL documents

### Output Structure
```
output_dir/
├── chunk_00000000.00000000.shuffled.jsonl.zst
├── chunk_00000001.00000000.shuffled.jsonl.zst
├── chunk_00000002.00000000.shuffled.jsonl.zst
├── chunk_00000002.00000001.shuffled.jsonl.zst  # Second file for chunk 2 if max_len exceeded
├── chunk_00000003.00000000.shuffled.jsonl.zst
└── ...
```

**Filename format:** `chunk_{CHUNK_ID}.{FILE_IDX}.shuffled.jsonl.zst`
- `CHUNK_ID`: Output chunk number (0 to num_outputs-1)
- `FILE_IDX`: Sequential index when a chunk exceeds max_len
- All files are zstd-compressed (level 3)

## How It Works

1. **Random Assignment**: Each document is assigned to a random output chunk (0 to num_outputs-1)
2. **Parallel Processing**: Multiple input files are processed simultaneously
3. **Streaming Writes**: Documents are written to their assigned chunk as they're processed
4. **File Rotation**: When a chunk exceeds `max_len`, a new file is created for that chunk
5. **No Internal Shuffling**: Documents maintain their relative order within each output file

### What This Does

- **Coarse Shuffling**: Distributes documents across many files
- **File-Level Randomization**: Breaks up patterns in original file organization
- **Parallel-Friendly Output**: Creates many output files suitable for parallel downstream processing

### What This Does NOT Do

- **Fine-Grained Shuffling**: Does not shuffle documents within each output file
- **Perfect Randomization**: Documents from the same input file may cluster in output files
- **Deterministic Output**: Random assignment means different runs produce different outputs

## Performance Characteristics

- **Speed**: Very fast, single-pass streaming operation
- **Memory Usage**: Low, only maintains `num_outputs` open file handles
- **Parallelism**: Input files processed in parallel across threads
- **Randomness**: Uses fast random number generation (fastrand)
- **I/O Pattern**: Random writes across `num_outputs` files

## Common Use Cases

### Prepare Data for Parallel Training
```bash
# Shuffle into 1000 files for distributed training
datamap shuffle \
  --input_dir ./processed_data \
  --output_dir ./training_shards \
  --num_outputs 1000 \
  --max_len 268435456
```

### Break Up Sequential Patterns
```bash
# Data is organized by date, shuffle to randomize
datamap shuffle \
  --input_dir ./data/by_date \
  --output_dir ./data/shuffled \
  --num_outputs 500
```

### Redistribute Before Sampling
```bash
# Shuffle before taking a sample to avoid bias
datamap shuffle \
  --input_dir ./full_dataset \
  --output_dir ./shuffled \
  --num_outputs 100

# Then sample from shuffled data
datamap reshard \
  --input_dir ./shuffled \
  --output_dir ./sampled \
  --subsample 0.1 \
  --max_size 268435456
```

### Prepare for Multi-Node Processing
```bash
# Create 100 shards for distribution across 10 nodes
datamap shuffle \
  --input_dir ./data \
  --output_dir ./distributed_shards \
  --num_outputs 100 \
  --delete_after_read
```

## Choosing `num_outputs`

### Considerations

- **Too Few**: Limited parallelism, may create very large files
- **Too Many**: File system overhead, many small files
- **Recommended**: 10-100x the number of parallel workers

### Guidelines

| Use Case | Recommended num_outputs |
|----------|------------------------|
| Single-machine training | 50-200 |
| Multi-node distributed training | 100-1000 |
| Preprocessing for sampling | 50-100 |
| Breaking up patterns | 100-500 |

## Combining with Other Commands

### Shuffle + Reshard (Fine-Grained Shuffle)

For true document-level shuffling, combine shuffle with reshard:
```bash
# Step 1: Coarse shuffle into many files
datamap shuffle \
  --input_dir ./data \
  --output_dir ./coarse_shuffled \
  --num_outputs 500

# Step 2: Reshard with subsample=1.0 to shuffle within files
datamap reshard \
  --input_dir ./coarse_shuffled \
  --output_dir ./fully_shuffled \
  --max_size 268435456 \
  --subsample 1.0
```

### Shuffle + Partition

Shuffle before partitioning to balance partition sizes:
```bash
# Step 1: Shuffle to break up clustering
datamap shuffle \
  --input_dir ./data \
  --output_dir ./shuffled \
  --num_outputs 200

# Step 2: Partition by quality
datamap range-partition \
  --input_dir ./shuffled \
  --output_dir ./partitioned \
  --value "metadata.quality_score" \
  --num_buckets 10
```

## Output Statistics

After completion, prints:
- Total number of documents shuffled
- Total number of output files created
- Total processing time

## Limitations

### Not a True Shuffle

This command performs **coarse shuffling only**. If you need true random shuffling at the document level:

1. Use external shuffle tools (GNU `shuf`, `sort -R`, etc.)
2. Combine with reshard for two-stage shuffling
3. Use specialized data pipeline tools

### Memory Considerations

- Maintains `num_outputs` open file handles simultaneously
- Each output file has a write buffer
- Memory usage scales with `num_outputs`
- System file handle limits may constrain `num_outputs`

### File System Limits

Most systems limit open file handles:
- Linux default: 1024 per process
- Recommended max `num_outputs`: 500-1000
- Use `ulimit -n` to check/adjust limits

## Technical Details

### Random Assignment

- Uses `fastrand` for fast random number generation
- Each document independently assigned: `chunk_id = random() % num_outputs`
- No guarantees on output file sizes (statistical distribution)

### Compression

- All output files compressed with zstd level 3
- Compression applied on-the-fly during writing
- No additional disk space required for uncompressed data

### File Rotation

When an output chunk exceeds `max_len`:
1. Current encoder is flushed and finished
2. New file created with incremented `FILE_IDX`
3. Writing continues to new file
4. Original chunk ID maintained

## Examples

### Basic Shuffle
```bash
datamap shuffle \
  --input_dir ./data \
  --output_dir ./shuffled \
  --num_outputs 100
```

### Shuffle with Cleanup
```bash
datamap shuffle \
  --input_dir ./data \
  --output_dir ./shuffled \
  --num_outputs 200 \
  --delete_after_read
```

### Shuffle into Smaller Files
```bash
datamap shuffle \
  --input_dir ./data \
  --output_dir ./shuffled \
  --num_outputs 500 \
  --max_len 104857600  # 100MB files
```

### High-Parallelism Shuffle
```bash
datamap shuffle \
  --input_dir ./data \
  --output_dir ./shuffled \
  --num_outputs 1000 \
  --threads 32
```

## Notes

- **Non-Deterministic**: Different runs produce different outputs
- **No Ordering Guarantees**: Documents may appear in any order in output files
- **Parallel Safe**: Multiple threads write to different chunks safely
- **Space Efficient**: Streaming operation, no intermediate storage required
- **Fast Operation**: Typically I/O bound, very little computation