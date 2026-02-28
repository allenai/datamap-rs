#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
CONFIG_FILE="configs/pdfs/classifiers/finepdfish.yaml"

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
# Define all input directories and compute subset for this instance
# ============================================================================

ALL_INPUT_DIRS=(
    "pretraining-data/sources/dolma4pdfs/olmo-crawled-pdfs_reshard_with_urls_wo_nospam_nopii_nobigtablesv5/step_final"
    "pretraining-data/sources/dolma4pdfs/s2orcforolmo_reshard_urltagged_fostagged_norefs_partitioned"
    "pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii_ufwtag"
)

get_output_dir() {
    local input_dir="$1"
    if [[ "${input_dir}" == */step_final ]]; then
        echo "${input_dir%/step_final}_qualitytagged"
    else
        echo "${input_dir}_qualitytagged"
    fi
}

# Compute which input dirs this instance should process.
# Each instance processes inputs where (index % world_size) == rank.
INPUT_DIRS=()
for i in "${!ALL_INPUT_DIRS[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq ${RANK} ]; then
        INPUT_DIRS+=("${ALL_INPUT_DIRS[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${INPUT_DIRS[*]}"

# ============================================================================
# Download classifier models referenced in config file
# ============================================================================

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "Config file ${CONFIG_FILE} not found"
    exit 1
fi

grep "fast_text_file:" "${CONFIG_FILE}" | awk '{print $2}' | sort -u | while read -r local_model_path; do
    local_model_dir=$(dirname "${local_model_path}")
    local_model_file="${local_model_dir}/model.bin"

    if [ -f "${local_model_file}" ]; then
        echo "Model ${local_model_file} already exists"
        continue
    fi

    mkdir -p "${local_model_dir}"
    s3_model_dir="${local_model_dir/${LOCAL_DIR}/${REMOTE_DIR}}"
    echo "Downloading model ${s3_model_dir}/model.bin"
    s5cmd cp -sp "${s3_model_dir}/model.bin" "${local_model_file}"
done

# download tokenizer
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json

# ============================================================================
# Process inputs: tagging
# ============================================================================

for input_dir in "${INPUT_DIRS[@]}"; do
    output_dir=$(get_output_dir "${input_dir}")

    local_input_dir="${LOCAL_DIR}/${input_dir}"
    local_output_dir="${LOCAL_DIR}/${output_dir}"

    if [ -d "${local_output_dir}" ]; then
        echo "Output directory ${local_output_dir} already exists"
        continue
    fi

    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${input_dir}"
        mkdir -p "${local_input_dir}"
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    echo "Tagging ${input_dir} with config ${CONFIG_FILE}..."
    cargo run --release map \
        --input-dir "${local_input_dir}" \
        --output-dir "${local_output_dir}" \
        --config "${CONFIG_FILE}"
done

# ============================================================================
# Process inputs: sampling
# ============================================================================

for input_dir in "${INPUT_DIRS[@]}"; do
    output_dir=$(get_output_dir "${input_dir}")
    tagged_step_final_dir="${LOCAL_DIR}/${output_dir}/step_final/"
    quality_report_file="${LOCAL_DIR}/${output_dir}/pdf_quality_report.yaml"
    gzip_report_file="${LOCAL_DIR}/${output_dir}/gzip_compression_report.yaml"

    if [ -f "${quality_report_file}" ]; then
        echo "Output file ${quality_report_file} already exists"
    else
        echo "Determining vigintiles for ${input_dir} quality score..."
        uv run python/percentile.py \
            "${tagged_step_final_dir}" \
            --output-file "${quality_report_file}" \
            --expression ".metadata.combined_quality_score" \
            --weight-by '.text | length' \
            --num-samples 10000000   # 10M samples
    fi

    if [ -f "${gzip_report_file}" ]; then
        echo "Output file ${gzip_report_file} already exists"
    else
        echo "Determining vigintiles for ${input_dir} gzip compression ratio..."
        uv run python/percentile.py \
            "${tagged_step_final_dir}" \
            --output-file "${gzip_report_file}" \
            --expression ".metadata.gzip_compression_ratio" \
            --weight-by '.text | length' \
            --num-samples 10000000   # 10M samples
    fi
done

# ============================================================================
# Upload results to S3
# ============================================================================

echo "Uploading results to S3..."
for input_dir in "${INPUT_DIRS[@]}"; do
    output_dir=$(get_output_dir "${input_dir}")
    local_dir="${LOCAL_DIR}/${output_dir}"
    s3_dir="${REMOTE_DIR}/${output_dir}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${input_dir} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
