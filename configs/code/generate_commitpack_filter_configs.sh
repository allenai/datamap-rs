#!/bin/bash

# Script to generate filter configs for all commitpack languages based on
# code_quality_report.yaml and gzip_compression_report.yaml from S3

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

FILTERS_DIR="${SCRIPT_DIR}/filters_commitpack"
S3_BASE="s3://ai2-llm/pretraining-data/sources/bigcode_commitpack/dolma-3_5-languages_tagged"

mkdir -p "${FILTERS_DIR}"

# Mapping from commitpack language name (used in S3 paths) to config output filename
declare -A LANG_MAP=(
    ["bluespec"]="bluespec"
    ["c"]="c"
    ["c#"]="csharp"
    ["c++"]="cpp"
    ["clojure"]="clojure"
    ["common-lisp"]="common_lisp"
    ["css"]="css"
    ["cuda"]="cuda"
    ["dart"]="dart"
    ["erlang"]="erlang"
    ["fortran"]="fortran"
    ["go"]="go"
    ["haskell"]="haskell"
    ["html"]="html"
    ["java"]="java"
    ["java-server-pages"]="java_server_pages"
    ["javascript"]="javascript"
    ["julia"]="julia"
    ["jupyter-notebook"]="jupyter_notebook"
    ["kotlin"]="kotlin"
    ["lua"]="lua"
    ["markdown"]="markdown"
    ["mathematica"]="mathematica"
    ["matlab"]="matlab"
    ["objective-c++"]="objective_cpp"
    ["ocaml"]="ocaml"
    ["opencl"]="opencl"
    ["pascal"]="pascal"
    ["perl"]="perl"
    ["php"]="php"
    ["python"]="python"
    ["r"]="r"
    ["restructuredtext"]="restructuredtext"
    ["rmarkdown"]="rmarkdown"
    ["ruby"]="ruby"
    ["rust"]="rust"
    ["scala"]="scala"
    ["scheme"]="scheme"
    ["scss"]="scss"
    ["shell"]="shell"
    ["sql"]="sql"
    ["swift"]="swift"
    ["systemverilog"]="systemverilog"
    ["tcl"]="tcl"
    ["typescript"]="typescript"
    ["vhdl"]="vhdl"
    ["vue"]="vue"
)

# Percentile keys in order (p5 to p95 in increments of 5)
PERCENTILES=(p5 p10 p15 p20 p25 p30 p35 p40 p45 p50 p55 p60 p65 p70 p75 p80 p85 p90 p95)

generate_filter_config() {
    local lang_key="$1"
    local config_name="$2"
    local output_file="${FILTERS_DIR}/${config_name}.yaml"

    if [[ -f "$output_file" ]]; then
        echo "Filter config for ${config_name} already exists"
        return 0
    fi

    echo "Generating filter config for ${config_name} (S3 lang: ${lang_key})..."

    # Fetch the code_quality_report.yaml from S3
    local quality_report
    quality_report=$(aws s3 cp "${S3_BASE}/${lang_key}/code_quality_report.yaml" - 2>/dev/null)

    if [[ -z "$quality_report" ]]; then
        echo "  ERROR: Could not fetch code_quality_report.yaml for ${lang_key}"
        return 1
    fi

    # Fetch the gzip_compression_report.yaml from S3
    local gzip_report
    gzip_report=$(aws s3 cp "${S3_BASE}/${lang_key}/gzip_compression_report.yaml" - 2>/dev/null)

    if [[ -z "$gzip_report" ]]; then
        echo "  ERROR: Could not fetch gzip_compression_report.yaml for ${lang_key}"
        return 1
    fi

    # Extract gzip compression percentiles for invalid_compression bounds
    local gzip_lower gzip_upper
    gzip_lower=$(yaml_get "$gzip_report" "value.percentiles.p1")
    gzip_upper=$(yaml_get "$gzip_report" "value.percentiles.p99")

    if [[ -z "$gzip_lower" ]] || [[ -z "$gzip_upper" ]]; then
        echo "  WARNING: Could not find gzip compression percentiles, using defaults"
        gzip_lower=0.1
        gzip_upper=1.0
    fi

    # Format gzip values to 6 decimal places
    gzip_lower=$(printf "%.6f" "$gzip_lower")
    gzip_upper=$(printf "%.6f" "$gzip_upper")

    # Start building the config file
    cat > "$output_file" << EOF
name: commitpack_filter
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

    # Extract quality percentiles and add float_filter entries
    for pct in "${PERCENTILES[@]}"; do
        local value
        value=$(yaml_get "$quality_report" "value.percentiles.${pct}")

        if [[ -z "$value" ]]; then
            echo "  WARNING: Could not find ${pct} in quality report for ${lang_key}"
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
          float_field: metadata.combined_quality_score
          lower_bound: ${value}
EOF
    done

    # catches top bin of quality
        cat >> "$output_file" << EOF
    - name: float_filter
      step: quality_p95
      kwargs:
          float_field: metadata.combined_quality_score
          lower_bound: 1000
EOF

    echo "  Created ${output_file}"
}

# Process all languages
for lang_key in "${!LANG_MAP[@]}"; do
    echo $lang_key
    config_name="${LANG_MAP[$lang_key]}"
    generate_filter_config "$lang_key" "$config_name"
done

echo "Done! Generated filter configs for ${#LANG_MAP[@]} languages."
