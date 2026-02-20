#!/bin/bash

# Script to generate filter configs for sponge code prose sources based on code_quality_report.yaml from S3

set -exou

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to extract a value from nested YAML using Python
yaml_get() {
    local yaml_content="$1"
    local key_path="$2"
    echo "$yaml_content" | uv run --with=pyyaml python -c "
import sys
import yaml
data = yaml.safe_load(sys.stdin)
keys = '${key_path}'.split('.')
val = data
for k in keys:
    if val is None:
        break
    val = val.get(k)
print(val if val is not None else '')
"
}

FILTERS_DIR="${SCRIPT_DIR}/filters_sponge_code_prose"
S3_BASE="s3://ai2-llm/pretraining-data/sources"

mkdir -p "${FILTERS_DIR}"

ALL_SOURCES=(
    "sponge_211_code_prose"
    "sponge_211_non-software-development_code_prose"
)

# Percentile keys in order (p5 to p95 in increments of 5)
PERCENTILES=(p5 p10 p15 p20 p25 p30 p35 p40 p45 p50 p55 p60 p65 p70 p75 p80 p85 p90 p95)

generate_filter_config() {
    local source="$1"
    local output_file="${FILTERS_DIR}/${source}.yaml"

    if [[ -f "$output_file" ]]; then
        echo "Filter config for ${source} already exists"
        return 0
    fi

    echo "Generating filter config for ${source}..."

    # Fetch the code_quality_report.yaml from S3
    local quality_report
    quality_report=$(aws s3 cp "${S3_BASE}/${source}_code_prose_tagged/code_quality_report.yaml" - 2>/dev/null)

    if [[ -z "$quality_report" ]]; then
        echo "  ERROR: Could not fetch code_quality_report.yaml for ${source}"
        return 1
    fi

    # Fetch the gzip_compression_report.yaml from S3
    local gzip_report
    gzip_report=$(aws s3 cp "${S3_BASE}/${source}_code_prose_tagged/gzip_compression_report.yaml" - 2>/dev/null)

    if [[ -z "$gzip_report" ]]; then
        echo "  ERROR: Could not fetch gzip_compression_report.yaml for ${source}"
        return 1
    fi

    # Extract gzip compression percentiles for bounds
    local gzip_lower gzip_upper
    gzip_lower=$(yaml_get "$gzip_report" "value.percentiles.p1")
    gzip_upper=$(yaml_get "$gzip_report" "value.percentiles.p99")

    if [[ -z "$gzip_lower" ]] || [[ -z "$gzip_upper" ]]; then
        echo "  ERROR: Could not find gzip compression percentiles for ${source}"
        return 1
    fi

    # Format gzip values to 6 decimal places
    gzip_lower=$(printf "%.6f" "$gzip_lower")
    gzip_upper=$(printf "%.6f" "$gzip_upper")

    # Start building the config file
    cat > "$output_file" << EOF
name: code_filter
text_field: text
pipeline:
    - name: float_filter  # things that don't compress well
      step: gzip_compression_p01
      kwargs:
          float_field: metadata.gzip_compression_ratio
          lower_bound: ${gzip_lower}
    - name: float_filter  # things that are super compressable
      step: gzip_compression_p99
      kwargs:
          float_field: metadata.gzip_compression_ratio
          upper_bound: ${gzip_upper}
EOF

    # Extract percentiles and add float_filter entries
    for pct in "${PERCENTILES[@]}"; do
        # Extract the percentile value from the nested YAML structure (value.percentiles.pXX)
        local value
        value=$(yaml_get "$quality_report" "value.percentiles.${pct}")

        if [[ -z "$value" ]]; then
            echo "  WARNING: Could not find ${pct} in report for ${source}"
            continue
        fi

        # Format to 6 decimal places
        value=$(printf "%.6f" "$value")

        # Remove 5 from the percentile number and format as two digits with "quality_p" prefix
        local pct_num="${pct#p}"
        local adjusted_num=$((pct_num - 5))
        display_pct=$(printf "quality_p%02d" "$adjusted_num")

        cat >> "$output_file" << EOF
    - name: float_filter
      step: ${display_pct}
      kwargs:
          float_field: metadata.code_prose_combined
          lower_bound: ${value}
EOF
    done

    # catches top bin of quality
        cat >> "$output_file" << EOF
    - name: float_filter
      step: quality_p95
      kwargs:
          float_field: metadata.code_prose_combined
          lower_bound: 1000
EOF

    echo "  Created ${output_file}"
}

# Process all sources
for source in "${ALL_SOURCES[@]}"; do
    generate_filter_config "$source"
done

echo "Done! Generated filter configs for ${#ALL_SOURCES[@]} sources."
