#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="/mnt/raid0"
INPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard"
OUTPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_fixed"
S3_OUTPUT_DIR="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_fixed"

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
    "Python"
    "C"
    "C++"
    "C-Sharp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Ruby"
    "Rust"
    "Shell"
    "SQL"
    "Swift"
    "TypeScript"
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

# ============================================================================
# Process languages: tagging
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    if [ "${language}" == "C++" ]; then
        config_file="configs/code/classifiers/cpp.yaml"
    elif [ "${language}" == "C-Sharp" ]; then
        config_file="configs/code/classifiers/csharp.yaml"
    else
        config_file="configs/code/classifiers/$(echo ${language} | tr '[:upper:]' '[:lower:]').yaml"
    fi

    input_dir="${INPUT_DIR}/${language}/step_final/"
    output_dir="${OUTPUT_DIR}/${language}/"

    if [ -d "${output_dir}" ]; then
        echo "Output directory ${output_dir} already exists"
        continue
    fi

    echo "Tagging ${language} with config ${config_file}..."
    cargo run --release map --input-dir ${input_dir} --output-dir ${output_dir} --config ${config_file}
done

# ============================================================================
# Process languages: sampling
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    input_dir="${OUTPUT_DIR}/${language}/step_final/"
    output_file="${OUTPUT_DIR}/${language}/code_quality_report.json"

    if [ -f "${output_file}" ]; then
        echo "Output file ${output_file} already exists"
        continue
    fi

    echo "Sampling ${language}..."
    cargo run --release reservoir-sample --input-dir ${input_dir} --output-file ${output_file} \
    --key "metadata.code_quality.__label__pos" --text-key text --token-weighted --reservoir-size 100000
done

# ============================================================================
# Upload results to S3
# ============================================================================

echo "Uploading results to S3..."
for language in "${LANGUAGES[@]}"; do
    local_dir="${OUTPUT_DIR}/${language}"
    s3_dir="${S3_OUTPUT_DIR}/${language}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${language} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
