#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
CONFIG_FILE="configs/code/classifiers_sponge_code_prose/markdown.yaml"

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

# If there's only one instance, force rank to 0
if [ "${WORLD_SIZE}" -eq 1 ]; then
    RANK=0
    echo "Single instance detected, setting RANK to 0"
fi

# ============================================================================
# Define all sources and compute subset for this instance
# ============================================================================

ALL_SOURCES=(
    "sponge_211_code_prose"
    "sponge_211_non-software-development_code_prose"
)

# Compute which sources this instance should process
# Each instance processes sources where (source_index % world_size) == rank
SOURCES=()
for i in "${!ALL_SOURCES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq ${RANK} ]; then
        SOURCES+=("${ALL_SOURCES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${SOURCES[*]}"

# download tokenizer
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json

# download the Markdown classifier
CLASSIFIER_S3="s3://ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5"
CLASSIFIER_LOCAL="/mnt/raid0/ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5"
s5cmd sync "${CLASSIFIER_S3}/Markdown/*" "${CLASSIFIER_LOCAL}/Markdown/"

# ============================================================================
# Process sources: tagging
# ============================================================================

for source in "${SOURCES[@]}"; do
    input_dir="pretraining-data/sources/${source}"
    output_dir="pretraining-data/sources/${source}_stack_edu_markdown_tagged"

    local_input_dir="${LOCAL_DIR}/${input_dir}"
    local_output_dir="${LOCAL_DIR}/${output_dir}"

    if [ -d "${local_output_dir}" ]; then
        echo "Output directory ${local_output_dir} already exists"
        continue
    fi

    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${input_dir}"
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    echo "Tagging ${source} with config ${CONFIG_FILE}..."
    cargo run --release map \
        --input-dir "${local_input_dir}" \
        --output-dir "${local_output_dir}" \
        --config "${CONFIG_FILE}"
done

# ============================================================================
# Process sources: sampling
# ============================================================================

for source in "${SOURCES[@]}"; do
    output_dir="pretraining-data/sources/${source}_stack_edu_markdown_tagged"
    input_dir="${LOCAL_DIR}/${output_dir}/step_final/"
    output_file="${LOCAL_DIR}/${output_dir}/code_quality_report.yaml"

    if [ -f "${output_file}" ]; then
        echo "Output file ${output_file} already exists"
        continue
    fi

    echo "Determining vigintiles for ${source}..."
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
for source in "${SOURCES[@]}"; do
    output_dir="pretraining-data/sources/${source}_stack_edu_markdown_tagged"
    local_dir="${LOCAL_DIR}/${output_dir}"
    s3_dir="${REMOTE_DIR}/${output_dir}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${source} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
