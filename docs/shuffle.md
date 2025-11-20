# Shuffle Command Documentation

## Overview

The Shuffle command coarsely redistributes data across a specified number of output files. Each document is randomly assigned to one of the output files, but the data within each output file is **not** shuffled internally. This provides a lightweight way to redistribute data across many files for parallel processing or to break up patterns in the original file organization.

## Usage
```bash
datamap shuffle \
  --input_dir ./data/input \
  --output_dir ./data/shuffled \
  --num_outputs 100 \
  [--max_len 256000000] \
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
# Shuffle into at least 1000 files for distributed training
datamap shuffle \
  --input_dir ./processed_data \
  --output_dir ./training_shards \
  --num_outputs 1000 \
  --max_len 256000000
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
- Use `ulimit -n` to check/adjust limits

