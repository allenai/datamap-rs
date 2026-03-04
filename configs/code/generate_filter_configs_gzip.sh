#!/bin/bash

# Script to generate filter configs for all stack languages based on
# code_quality_report.yaml and gzip_compression_report.yaml from S3.

set -euox pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to extract a value from nested YAML using Python.
yaml_get() {
    local yaml_content="$1"
    local key_path="$2"
    echo "$yaml_content" | uv run --with=pyyaml python -c "
import sys
import yaml

data = yaml.safe_load(sys.stdin)
keys = '${key_path}'.split('.')
val = data
for key in keys:
    if val is None:
        break
    val = val.get(key)
print(val if val is not None else '')
"
}

FILTERS_DIR="${SCRIPT_DIR}/filters_gzip"
S3_BASE="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged"

mkdir -p "${FILTERS_DIR}"

ALL_LANGUAGES=(
    "Blade"
    "Bluespec"
    "C"
    "C-Sharp"
    "C++"
    "Clojure"
    "Common_Lisp"
    "CSS"
    "Cuda"
    "Dart"
    "Erlang"
    "Fortran"
    "Fortran_Free_Form"
    "Go"
    "Haskell"
    "html"
    "Java"
    "Java_Server_Pages"
    "JavaScript"
    "Julia"
    "jupyter_notebook"
    "Kotlin"
    "Lua"
    "Markdown"
    "Mathematica"
    "MATLAB"
    "Objective-C"
    "OCaml"
    "OpenCL"
    "Pascal"
    "Perl"
    "PHP"
    "Python"
    "R"
    "reStructuredText"
    "RMarkdown"
    "Ruby"
    "Rust"
    "Scala"
    "Scheme"
    "SCSS"
    "Shell"
    "SQL"
    "Swift"
    "SystemVerilog"
    "Tcl"
    "TypeScript"
    "Verilog"
    "VHDL"
    "Vue"
)

# Percentile keys in order (p5 to p95 in increments of 5).
PERCENTILES=(p5 p10 p15 p20 p25 p30 p35 p40 p45 p50 p55 p60 p65 p70 p75 p80 p85 p90 p95)

config_name_for_language() {
    local language="$1"
    case "${language}" in
        "C++")
            echo "cpp"
            ;;
        "C-Sharp")
            echo "csharp"
            ;;
        "Objective-C")
            echo "objective_c"
            ;;
        *)
            echo "${language}" | tr '[:upper:]' '[:lower:]' | tr '-' '_'
            ;;
    esac
}

generate_filter_config() {
    local language="$1"
    local config_name
    config_name="$(config_name_for_language "${language}")"
    local output_file="${FILTERS_DIR}/${config_name}.yaml"

    if [[ -f "${output_file}" ]]; then
        echo "Filter config for ${config_name} already exists"
        return 0
    fi

    echo "Generating filter config for ${language} (${config_name})..."

    # Fetch the code_quality_report.yaml from S3.
    local quality_report
    quality_report=$(aws s3 cp "${S3_BASE}/${language}/code_quality_report.yaml" - 2>/dev/null || true)

    if [[ -z "${quality_report}" ]]; then
        echo "  ERROR: Could not fetch code_quality_report.yaml for ${language}"
        return 1
    fi

    # Fetch the gzip_compression_report.yaml from S3.
    local gzip_report
    gzip_report=$(aws s3 cp "${S3_BASE}/${language}/gzip_compression_report.yaml" - 2>/dev/null || true)

    if [[ -z "${gzip_report}" ]]; then
        echo "  ERROR: Could not fetch gzip_compression_report.yaml for ${language}"
        return 1
    fi

    # Extract gzip compression percentiles for bounds.
    local gzip_lower gzip_upper
    gzip_lower=$(yaml_get "${gzip_report}" "value.percentiles.p1")
    gzip_upper=$(yaml_get "${gzip_report}" "value.percentiles.p99")

    if [[ -z "${gzip_lower}" ]] || [[ -z "${gzip_upper}" ]]; then
        echo "  ERROR: Could not find gzip compression percentiles for ${language}"
        return 1
    fi

    gzip_lower=$(printf "%.6f" "${gzip_lower}")
    gzip_upper=$(printf "%.6f" "${gzip_upper}")

    # Start building the config file with gzip quality guards.
    cat > "${output_file}" << EOF
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

    # Add stack_edu_redux quality thresholds.
    for pct in "${PERCENTILES[@]}"; do
        local value
        value=$(yaml_get "${quality_report}" "value.percentiles.${pct}")

        if [[ -z "${value}" ]]; then
            echo "  WARNING: Could not find ${pct} in quality report for ${language}"
            continue
        fi

        value=$(printf "%.6f" "${value}")

        local pct_num="${pct#p}"
        local adjusted_num=$((pct_num - 5))
        local display_pct
        display_pct=$(printf "quality_p%02d" "${adjusted_num}")

        cat >> "${output_file}" << EOF
    - name: float_filter
      step: ${display_pct}
      kwargs:
          float_field: metadata.stack_edu_redux_combined
          lower_bound: ${value}
EOF
    done

    # Catch top bin of quality.
    cat >> "${output_file}" << EOF
    - name: float_filter
      step: quality_p95
      kwargs:
          float_field: metadata.stack_edu_redux_combined
          lower_bound: 1000
EOF

    echo "  Created ${output_file}"
}

for language in "${ALL_LANGUAGES[@]}"; do
    generate_filter_config "${language}"
done

echo "Done! Generated filter configs for ${#ALL_LANGUAGES[@]} languages."
