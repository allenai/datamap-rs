# Partition Commands Documentation

## Overview

DataMap provides two partitioning commands for organizing data into subdirectories based on specific criteria:

- **Discrete Partition**: Partitions data based on categorical values (discrete support)
- **Range Partition**: Partitions data based on continuous numeric values (continuous support)

Both commands are useful for organizing large datasets by quality, language, domain, or other characteristics.

---

## Discrete Partition

### Overview

Discrete Partition organizes data into subdirectories based on categorical field values. It's ideal for partitioning by fields with a small number of distinct categories (e.g., language codes, domains, content types).

### Usage
```bash
datamap discrete-partition \
  --input_dir ./data/input \
  --output_dir ./data/partitioned \
  [--config partition_config.yaml] \
  [--partition_key "metadata.language"] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_dir`: Directory for partitioned output
- `--config`: (Optional) Path to YAML configuration file
- `--partition_key`: (Optional) Field to partition on (alternative to config file)
- `--threads`: (Optional) Number of threads to use (default: all available cores)

**Note**: Either `--config` or `--partition_key` must be provided.

### Configuration
```yaml
name: "Language Partition"
partition_key: "metadata.language"
choices: ["en", "es", "fr", "de"]  # Optional: predefined categories
max_file_size: 256000000  # Optional: max bytes per output file (default: 256MB)
```

#### Configuration Fields

- `name`: Descriptive name for the partition operation
- `partition_key`: JSON field path to partition on (e.g., "metadata.language", "url", "domain")
- `choices`: (Optional) List of valid categories. Documents not matching these go to "no_category"
- `max_file_size`: (Optional) Maximum uncompressed bytes per output file (default: 268435456 = 256MB)

### Input/Output Format

#### Input
- JSONL files in any format
- Each document must contain the field specified by `partition_key`

#### Output Structure

**Without predefined choices:**
```
output_dir/
├── category1/
│   ├── chunk_00000000.jsonl.zst
│   ├── chunk_00000001.jsonl.zst
│   └── ...
├── category2/
│   ├── chunk_00000000.jsonl.zst
│   └── ...
└── no_category/  # Documents with null or missing partition key
    └── chunk_00000000.jsonl.zst
```

**With predefined choices:**
```
output_dir/
├── en/
│   ├── chunk_00000000.jsonl.zst
│   └── ...
├── es/
│   ├── chunk_00000000.jsonl.zst
│   └── ...
├── fr/
│   └── chunk_00000000.jsonl.zst
└── no_category/  # Documents not matching choices
    └── chunk_00000000.jsonl.zst
```

### Behavior

- **Dynamic Categories**: If `choices` is not specified, creates directories for all encountered values
- **Predefined Categories**: If `choices` is specified, only creates directories for listed categories
- **Null Handling**: Documents with null or missing partition keys go to `no_category/`
- **File Splitting**: Automatically creates new chunk files when `max_file_size` is reached

### Common Use Cases

#### Partition by Language
```yaml
name: "Language Partition"
partition_key: "metadata.language"
choices: ["en", "es", "fr", "de", "zh", "ja"]
max_file_size: 268435456
```

#### Partition by Domain
```bash
datamap discrete-partition \
  --input_dir ./web_data \
  --output_dir ./by_domain \
  --partition_key "domain"
```

#### Partition by Content Type
```yaml
name: "Content Type Partition"
partition_key: "metadata.content_type"
choices: ["article", "forum", "social", "news"]
```

### Output Statistics

After completion, prints:
- Total processing time
- Total number of documents processed
- Number of documents in each category

---

## Range Partition

### Overview

Range Partition organizes data into subdirectories based on continuous numeric values. It divides the data into buckets based on either predefined ranges or automatically calculated quantiles from a reservoir sample.

### Usage
```bash
datamap range-partition \
  --input_dir ./data/input \
  --output_dir ./data/partitioned \
  [--config partition_config.yaml] \
  [--value "metadata.quality_score"] \
  [--default_value 0.0] \
  [--range_groups 0.25,0.5,0.75] \
  [--reservoir_path ./stats/sample.json] \
  [--num_buckets 10] \
  [--max_file_size 268435456] \
  [--bucket_name "bucket"] \
  [--threads 16]
```

### Arguments

- `--input_dir`: Directory containing input JSONL files
- `--output_dir`: Directory for partitioned output
- `--config`: (Optional) Path to YAML configuration file
- `--value`: (Optional) Numeric field to partition on
- `--default_value`: (Optional) Default value for missing fields (default: 0.0)
- `--range_groups`: (Optional) Comma-separated list of boundary values
- `--reservoir_path`: (Optional) Path to reservoir sample JSON (for automatic quantile calculation)
- `--num_buckets`: (Optional) Number of buckets when using reservoir sample
- `--max_file_size`: (Optional) Max bytes per output file (default: 256MB)
- `--bucket_name`: (Optional) Prefix for bucket directories (default: "bucket")
- `--threads`: (Optional) Number of threads to use (default: all available cores)

**Note**: Either provide `range_groups` OR both `reservoir_path` and `num_buckets`.

### Configuration
```yaml
name: "Quality Score Partition"
value: "metadata.quality_score"
default_value: 0.0  # Optional: default for missing values
range_groups: [0.25, 0.5, 0.75]  # Option 1: Manual ranges
# OR
reservoir_path: "./stats/quality_sample.json"  # Option 2: Automatic from sample
num_buckets: 10
max_file_size: 268435456  # Optional: 256MB default
bucket_name: "quality"  # Optional: "bucket" default
```

#### Configuration Fields

- `name`: Descriptive name for the partition operation
- `value`: JSON field path containing numeric values to partition on
- `default_value`: (Optional) Value to use when field is missing or null (default: 0.0)
- `range_groups`: (Optional) List of boundary values defining ranges
- `reservoir_path`: (Optional) Path to reservoir sample for automatic quantile calculation
- `num_buckets`: (Optional) Number of buckets when using reservoir sample
- `max_file_size`: (Optional) Maximum uncompressed bytes per output file
- `bucket_name`: (Optional) Prefix for bucket directory names

### Input/Output Format

#### Input
- JSONL files in any format
- Documents should contain the numeric field specified by `value`
- Missing values use `default_value`

#### Output Structure
```
output_dir/
├── bucket_0000/
│   ├── shard_00000000.jsonl.zst
│   ├── shard_00000001.jsonl.zst
│   └── ...
├── bucket_0001/
│   ├── shard_00000000.jsonl.zst
│   └── ...
├── bucket_0002/
│   └── shard_00000000.jsonl.zst
└── ...
```

### How Range Partitioning Works

#### Manual Ranges (`range_groups`)

Given `range_groups: [0.25, 0.5, 0.75]`, creates 4 buckets:
- **bucket_0000**: `(-∞, 0.25)`
- **bucket_0001**: `[0.25, 0.5)`
- **bucket_0002**: `[0.5, 0.75)`
- **bucket_0003**: `[0.75, ∞)`

#### Automatic Quantiles (`reservoir_path` + `num_buckets`)

1. Loads reservoir sample from specified path
2. Sorts values
3. Calculates quantile boundaries to create approximately equal-sized buckets
4. Creates `num_buckets` directories with automatic boundaries

**Example with `num_buckets: 4`:**
- Calculates 25th, 50th, and 75th percentiles from sample
- Creates 4 buckets at these boundaries

### Common Use Cases

#### Quality-Based Partitioning with Manual Ranges
```yaml
name: "Quality Tiers"
value: "metadata.quality_score"
default_value: 0.0
range_groups: [0.3, 0.6, 0.8]  # Low, medium, high, very high quality
bucket_name: "quality"
```

#### Automatic Quantile Partitioning
```bash
# Step 1: Create reservoir sample
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/quality_sample.json \
  --key "metadata.quality_score" \
  --reservoir_size 100000

# Step 2: Partition using sample
datamap range-partition \
  --input_dir ./data \
  --output_dir ./partitioned \
  --value "metadata.quality_score" \
  --reservoir_path ./stats/quality_sample.json \
  --num_buckets 10 \
  --bucket_name "quality_decile"
```

#### Token-Weighted Quantile Partitioning
```bash
# Step 1: Create token-weighted sample
datamap reservoir-sample \
  --input_dir ./data \
  --output_file ./stats/quality_sample.json \
  --key "metadata.quality_score" \
  --reservoir_size 100000 \
  --token_weighted \
  --text_key "text"

# Step 2: Partition using token-weighted quantiles
datamap range-partition \
  --input_dir ./data \
  --output_dir ./partitioned \
  --reservoir_path ./stats/quality_sample.json \
  --num_buckets 5
```

### Output Statistics

After completion, prints:
- Total processing time
- Number of documents in each bucket
- Range boundaries for each bucket (e.g., `[0.25, 0.5) | 12,345 docs`)

---

## Performance Characteristics

Both partition commands share these performance characteristics:

- **Parallel Processing**: Multiple input files processed simultaneously
- **Memory Efficiency**: Files processed in streaming fashion
- **Automatic Splitting**: Output files split at `max_file_size` boundary
- **Compression**: All output files are zstd-compressed (level 3)

## Choosing Between Discrete and Range Partition

Use **Discrete Partition** when:
- Field has categorical/discrete values
- Small number of distinct categories (typically < 1000)
- Need to separate by language, domain, content type, etc.

Use **Range Partition** when:
- Field has continuous numeric values
- Want to create quality tiers or quantile buckets
- Need approximately equal-sized partitions (with reservoir sampling)
- Working with scores, probabilities, or other continuous metrics