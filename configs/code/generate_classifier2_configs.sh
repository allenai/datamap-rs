#!/bin/bash

# Script to generate classifier configs for batch 2 languages based on calibration.yaml from S3

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLASSIFIERS_DIR="${SCRIPT_DIR}/classifiers2"
S3_BASE="s3://ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5"
LOCAL_MODEL_BASE="/mnt/raid0/ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5"

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
    ["html"]="HTML"
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
)

generate_classifier_config() {
    local lang_key="$1"
    local s3_name="$2"
    local output_file="${CLASSIFIERS_DIR}/${lang_key}.yaml"

    echo "Generating classifier config for ${lang_key} (S3: ${s3_name})..."

    # Fetch the calibration.yaml from S3
    local calibration
    calibration=$(aws s3 cp "${S3_BASE}/${s3_name}/calibration.yaml" - 2>/dev/null)

    if [[ -z "$calibration" ]]; then
        echo "  ERROR: Could not fetch calibration.yaml for ${s3_name}"
        return 1
    fi

    # Extract the linear_transform_annotator kwargs (features + bias) from calibration.yaml
    local transform_section
    transform_section=$(echo "$calibration" | uv run --with=pyyaml python -c "
import sys
import yaml

data = yaml.safe_load(sys.stdin)

# calibration.yaml has 'weights' dict (__label__binN -> float) and 'bias'
weights = data.get('weights', {})
component_names = data.get('component_names', sorted(weights.keys()))
bias = data.get('bias', 0.0)

# Emit features block
lines = []
lines.append('          features:')
for name in component_names:
    w = weights[name]
    lines.append(f'              - field: metadata.stack_edu_redux.{name}')
    lines.append(f'                weight: {w}')
lines.append(f'          bias: {bias}')
lines.append('          output_field: metadata.stack_edu_redux_combined')

print('\n'.join(lines))
")

    # Write the config file
    cat > "$output_file" << EOF
name: ${lang_key}_code_classifier
text_field: text
pipeline:
    - name: gzip_annotator
      kwargs:
          text_field: text
          anno_field: metadata.gzip_compression_ratio

    - name: ultrafineweb_annotator
      kwargs:
          text_field: text
          output_field: metadata.stack_edu_redux
          max_text_length: 10000
          fast_text_file: ${LOCAL_MODEL_BASE}/${s3_name}/model.bin
    - name: linear_transform_annotator
      kwargs:
${transform_section}
EOF

    echo "  Created ${output_file}"
}

# Create classifiers directory if it doesn't exist
mkdir -p "$CLASSIFIERS_DIR"

# Process all languages
for lang_key in "${!LANG_MAP[@]}"; do
    s3_name="${LANG_MAP[$lang_key]}"
    generate_classifier_config "$lang_key" "$s3_name"
done

echo "Done! Generated classifier configs for ${#LANG_MAP[@]} languages."
