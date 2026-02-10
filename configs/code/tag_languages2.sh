#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
INPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_2026"
OUTPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged"
CONFIGS_DIR="configs/code/classifiers2"

# ============================================================================
# Get instance rank and world size from EC2 metadata
# ============================================================================

# Get instance ID from EC2 metadata
INSTANCE_ID=$(ec2-metadata --instance-id | grep -oP 'instance-id: \K(i-[a-f0-9]+)')
echo "Instance ID: ${INSTANCE_ID}"

# Get instance name using AWS CLI
INSTANCE_NAME=$(aws ec2 describe-tags \
    --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=Name" \
    --query "Tags[0].Value" --output text)
echo "Instance Name: ${INSTANCE_NAME}"

# Extract group name (everything except the last 4 digits) and instance index (last 4 digits)
# Instance name format: <group name>-<4 digit index>, e.g., "my-group-0001"
GROUP_NAME=$(echo "${INSTANCE_NAME}" | sed 's/-[0-9]\{4\}$//')
INSTANCE_IDX=$(echo "${INSTANCE_NAME}" | grep -oP '[0-9]{4}$')
# Remove leading zeros to get numeric rank
RANK=$((10#${INSTANCE_IDX}))
echo "Group Name: ${GROUP_NAME}"
echo "Instance Index: ${INSTANCE_IDX} (Rank: ${RANK})"

# Get world size by counting all instances with the same group name prefix
WORLD_SIZE=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${GROUP_NAME}-*" "Name=instance-state-name,Values=running" \
    --query "Reservations[*].Instances[*].InstanceId" --output text | wc -w)
echo "World Size: ${WORLD_SIZE}"

# ============================================================================
# Define all languages and compute subset for this instance
# ============================================================================

ALL_LANGUAGES=(
    "Blade"
    "Bluespec"
    "CSS"
    "Clojure"
    "CoNLL-U"
    "Common_Lisp"
    "Cuda"
    "Dart"
    "Erlang"
    "Fortran"
    "Fortran_Free_Form"
    "Haskell"
    "Java_Server_Pages"
    "Julia"
    "Kotlin"
    "Lua"
    "MATLAB"
    "Mathematica"
    "OCaml"
    "Objective-C"
    "OpenCL"
    "Pascal"
    "Perl"
    "R"
    "RMarkdown"
    "SCSS"
    "Scala"
    "Scheme"
    "SystemVerilog"
    "Tcl"
    "VHDL"
    "Verilog"
    "Vue"
    "html"
    "jupyter_notebook"
    "reStructuredText"
)

# Compute which languages this instance should process
# Each instance processes languages where (language_index % world_size) == rank
LANGUAGES=()
for i in "${!ALL_LANGUAGES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq ${RANK} ]; then
        LANGUAGES+=("${ALL_LANGUAGES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${LANGUAGES[*]}"

# download tokenizer
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json

# download classifiers for assigned languages only
CLASSIFIER_S3="s3://ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5"
CLASSIFIER_LOCAL="/mnt/raid0/ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5"
for language in "${LANGUAGES[@]}"; do
    s5cmd sync "${CLASSIFIER_S3}/${language}/*" "${CLASSIFIER_LOCAL}/${language}/"
done

# ============================================================================
# Process languages: tagging
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    if [ "${language}" == "Objective-C" ]; then
        config_name="objective_c.yaml"
    else
        config_name="$(echo ${language} | tr '[:upper:]' '[:lower:]').yaml"
    fi

    config_file="${CONFIGS_DIR}/${config_name}"

    if [ ! -f "${config_file}" ]; then
        echo "Config file ${config_file} not found... Skipping ${language}"
        continue
    fi

    local_input_dir="${LOCAL_DIR}/${INPUT_DIR}/${language}"
    local_output_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"

    if [ -d "${local_output_dir}" ]; then
        echo "Output directory ${local_output_dir} already exists"
        continue
    fi

    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${INPUT_DIR}/${language}"

        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    echo "Tagging ${language} with config ${config_file}..."
    cargo run --release map \
        --input-dir "${local_input_dir}" \
        --output-dir "${local_output_dir}" \
        --config "${config_file}"
done

# ============================================================================
# Process languages: sampling
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    input_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}/step_final/"
    output_file="${LOCAL_DIR}/${OUTPUT_DIR}/${language}/code_quality_report.yaml"

    if [ -f "${output_file}" ]; then
        echo "Output file ${output_file} already exists"
        continue
    fi

    echo "Determining vigintiles for ${language}..."
    uv run python/percentile.py \
        "${input_dir}" \
        --output-file "${output_file}" \
        --expression ".metadata.stack_edu_redux_combined" \
        --weight-by '.text | length' \
        --num-samples 10000000   # 10M samples
done

# ============================================================================
# Upload results to S3
# ============================================================================

echo "Uploading results to S3..."
for language in "${LANGUAGES[@]}"; do
    local_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"
    s3_dir="${REMOTE_DIR}/${OUTPUT_DIR}/${language}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${language} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
