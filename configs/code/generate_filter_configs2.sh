#!/bin/bash

# Script to generate filter configs for all languages based on code_quality_report.yaml from S3

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

FILTERS_DIR="${SCRIPT_DIR}/filters2"
S3_BASE="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged"

mkdir -p "${FILTERS_DIR}"

# Mapping from classifier name (lowercase) to S3 folder name
declare -A LANG_MAP=(
    ["blade"]="Blade"
    ["bluespec"]="Bluespec"
    ["clojure"]="Clojure"
    ["common_lisp"]="Common_Lisp"
    ["css"]="CSS"
    ["cuda"]="Cuda"
    ["dart"]="Dart"
    ["erlang"]="Erlang"
    ["fortran"]="Fortran"
    ["fortran_free_form"]="Fortran_Free_Form"
    ["haskell"]="Haskell"
    ["html"]="html"
    ["java_server_pages"]="Java_Server_Pages"
    ["julia"]="Julia"
    ["kotlin"]="Kotlin"
    ["lua"]="Lua"
    ["mathematica"]="Mathematica"
    ["matlab"]="MATLAB"
    ["objective_c"]="Objective-C"
    ["ocaml"]="OCaml"
    ["opencl"]="OpenCL"
    ["pascal"]="Pascal"
    ["perl"]="Perl"
    ["r"]="R"
    ["rmarkdown"]="RMarkdown"
    ["scala"]="Scala"
    ["scheme"]="Scheme"
    ["scss"]="SCSS"
    ["systemverilog"]="SystemVerilog"
    ["tcl"]="Tcl"
    ["verilog"]="Verilog"
    ["vhdl"]="VHDL"
    ["vue"]="Vue"
    ["jupyter_notebook"]="jupyter_notebook"
    ["restructuredtext"]="reStructuredText"
)

# Percentile keys in order (p5 to p95 in increments of 5)
PERCENTILES=(p5 p10 p15 p20 p25 p30 p35 p40 p45 p50 p55 p60 p65 p70 p75 p80 p85 p90 p95)

generate_filter_config() {
    local lang_key="$1"
    local s3_name="$2"
    local output_file="${FILTERS_DIR}/${lang_key}.yaml"

    if [[ -f "$output_file" ]]; then
        echo "Filter config for ${lang_key} already exists"
        return 0
    fi

    echo "Generating filter config for ${lang_key} (S3: ${s3_name})..."

    # Fetch the code_quality_report.yaml from S3
    local report
    report=$(aws s3 cp "${S3_BASE}/${s3_name}/code_quality_report.yaml" - 2>/dev/null)

    if [[ -z "$report" ]]; then
        echo "  ERROR: Could not fetch code_quality_report.yaml for ${s3_name}"
        return 1
    fi

    # Extract length percentiles for text_len_filter bounds
    local len_lower len_upper
    len_lower=$(yaml_get "$report" "length.percentiles.p1")
    len_upper=$(yaml_get "$report" "length.percentiles.p99")

    if [[ -z "$len_lower" ]] || [[ -z "$len_upper" ]]; then
        echo "  WARNING: Could not find length percentiles, using defaults"
        len_lower=32
        len_upper=262144
    fi

    # Start building the config file
    cat > "$output_file" << EOF
name: code_filter
text_field: text
pipeline:
    - name: text_len_filter  # p1-p99
      kwargs:
          text_field: text
          lower_bound: ${len_lower}
          upper_bound: ${len_upper}
EOF

    # Extract percentiles and add float_filter entries
    for pct in "${PERCENTILES[@]}"; do
        # Extract the percentile value from the nested YAML structure (value.percentiles.pXX)
        local value
        value=$(yaml_get "$report" "value.percentiles.${pct}")

        if [[ -z "$value" ]]; then
            echo "  WARNING: Could not find ${pct} in report for ${s3_name}"
            continue
        fi

        # Format to 6 decimal places
        value=$(printf "%.6f" "$value")

        cat >> "$output_file" << EOF
    - name: float_filter  # ${pct}
      kwargs:
          float_field: metadata.stack_edu_redux_combined
          lower_bound: ${value}
EOF
    done

    echo "  Created ${output_file}"
}

# Create filters directory if it doesn't exist
mkdir -p "$FILTERS_DIR"

# Process all languages
for lang_key in "${!LANG_MAP[@]}"; do
    echo $lang_key
    s3_name="${LANG_MAP[$lang_key]}"
    generate_filter_config "$lang_key" "$s3_name"
done

echo "Done! Generated filter configs for ${#LANG_MAP[@]} languages."
