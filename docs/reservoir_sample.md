# Reservoir Sample Command Documentation

## Overview

The Reservoir Sample command gathers statistics about data through distributed reservoir sampling. This command creates a representative sample of values from a large dataset, which is particularly useful for understanding data distributions before partitioning or for quality analysis. Supports both uniform sampling and token-weighted sampling.

## Usage
```bash
datamap reservoir-sample \
  --input_dir ./data/input \
  --output_file ./stats/sample.json \
  --key "metadata.quality_score" \
  --reservoir_size 100000 \
  [--token_weighted] \
  [--text_key "text"] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_file`: Path to output JSON file containing the sample
- `--key`: JSON field to sample (e.g., "metadata.quality_score", "url", "metadata.language")
- `--reservoir_size`: Number of items to include in the sample (default: 100000)
- `--token_weighted`: (Optional) Use token-weighted sampling instead of uniform sampling
- `--text_key`: (Optional) Text field for tokenization when using token-weighted sampling (default: "text")
- `--threads`: (Optional) Number of threads to use (default: all available cores)

## Input/Output Format

### Input
- JSONL files in any format (`.jsonl`, `.jsonl.gz`, `.jsonl.zst`, etc.)
- Each document must contain the field specified by `--key`
- For token-weighted sampling, documents must also contain the field specified by `--text_key`

### Output Format

**Uniform Sampling:**
```json
[
  value1,
  value2,
  value3,
  ...
]
```

**Token-Weighted Sampling:**
```json
[
  {"percentile": 0.05, "value": 0.123},
  {"percentile": 0.15, "value": 0.234},
  {"percentile": 0.25, "value": 0.345},
  ...
]
```

- Uniform sampling outputs a simple array of sampled values
- Token-weighted sampling outputs an array of objects with percentile information
- Values are sorted in ascending order for token-weighted sampling

## Sampling Methods

### Uniform Sampling (Default)

Standard reservoir sampling where each document has equal probability of being included:
- Every document has equal weight
- Produces a uniformly random sample of the specified size
- Good for general statistics and distribution analysis
- Fast and memory-efficient

### Token-Weighted Sampling (`--token_weighted`)

Samples documents proportionally to their token count using the cl100k_base tokenizer:
- Documents with more tokens have higher probability of inclusion
- Uses Algorithm A-Res (weighted reservoir sampling)
- Ideal for understanding quality distributions when training on tokens
- Output includes percentile information based on cumulative token weight
- Useful for range partitioning based on token-weighted distributions

## How It Works

1. **Parallel Processing**: Input files are distributed across threads
2. **Target Allocation**: Each thread is allocated a proportional reservoir size based on data size
3. **Reservoir Sampling**: Each thread maintains its own reservoir using classic reservoir sampling algorithm
4. **Aggregation**: Thread reservoirs are combined into final output
5. **Sorting**: For token-weighted sampling, results are sorted and percentiles are calculated

### Uniform Sampling Algorithm
- Processes documents sequentially
- For document `n`, generates random number `r` in `[0, n]`
- If `r < reservoir_size`, document value replaces item at index `r` in reservoir
- Ensures each document has equal probability of being in final sample

### Token-Weighted Sampling Algorithm
- Uses priority queue with log-space keys: `log(U) / weight`
- Documents with more tokens (higher weight) get higher priority
- Maintains heap of top `reservoir_size` items by priority
- Produces sample weighted by token count

## Performance Characteristics

- **Memory Usage**: Scales with `reservoir_size`, not dataset size
- **Processing Speed**: Single pass through data
- **Parallelism**: Multiple threads process different files simultaneously
- **Tokenization**: Token-weighted mode requires tokenization (slower than uniform)

## Common Use Cases

### Sample Quality Scores for Analysis
```bash
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/quality_sample.json \
  --key "metadata.quality_score" \
  --reservoir_size 100000
```

### Token-Weighted Quality Distribution
```bash
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/quality_percentiles.json \
  --key "metadata.quality_score" \
  --reservoir_size 100000 \
  --token_weighted \
  --text_key "text"
```

### Sample URLs for Domain Analysis
```bash
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/url_sample.json \
  --key "url" \
  --reservoir_size 50000
```

### Language Distribution Sampling
```bash
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/language_sample.json \
  --key "metadata.language" \
  --reservoir_size 200000
```

## Using Results for Range Partitioning

Token-weighted sampling output is designed to work directly with the Range Partition command:
```bash
# Step 1: Generate token-weighted sample
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/quality_sample.json \
  --key "metadata.quality_score" \
  --reservoir_size 100000 \
  --token_weighted

# Step 2: Use sample for range partitioning
datamap range-partition \
  --input_dir ./data \
  --output_dir ./partitioned \
  --reservoir_path ./stats/quality_sample.json \
  --num_buckets 10
```

## Output Statistics

After completion, the command prints:
- Final reservoir size
- Total number of documents or tokens processed

## Notes

- Reservoir sampling provides statistical guarantees of uniform randomness
- Token-weighted sampling uses the tiktoken cl100k_base tokenizer (GPT-4 tokenizer)
- Results are deterministic per-thread but may vary slightly between runs due to thread scheduling
- For stable results across runs, consider using single-threaded mode (`--threads 1`)
- Larger reservoir sizes provide more accurate distribution estimates but use more memory