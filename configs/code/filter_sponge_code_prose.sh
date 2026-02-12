#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
CONFIGS_DIR="configs/code/filters_sponge_code_prose"

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

# ============================================================================
# Process sources: filtering
# ============================================================================

for source in "${SOURCES[@]}"; do
    config_file="${CONFIGS_DIR}/${source}.yaml"

    if [ ! -f "${config_file}" ]; then
        echo "Config file ${config_file} not found... Skipping ${source}"
        continue
    fi

    input_dir="pretraining-data/sources/${source}_stack_edu_markdown_tagged"
    output_dir="pretraining-data/sources/${source}_stack_edu_markdown_tagged_partitioned"

    local_input_dir="${LOCAL_DIR}/${input_dir}"
    local_output_dir="${LOCAL_DIR}/${output_dir}"

    if [ -d "${local_output_dir}" ]; then
        echo "Output directory ${local_output_dir} already exists"
        continue
    fi

    # download files
    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${input_dir}"
        if ! s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"; then
            echo "Failed to download ${source} from ${remote_input_dir}... Skipping"
            continue
        fi
    fi

    # until there's a directory called "step_final", replace local_input_dir with step final;
    # otherwise break out of the loop.
    while true; do
        if [ -d "${local_input_dir}/step_final" ]; then
            local_input_dir="${local_input_dir}/step_final"
        else
            break
        fi
    done

    echo "Filtering ${source} with config ${config_file}..."
    cargo run --release map \
        --input-dir "${local_input_dir}" \
        --output-dir "${local_output_dir}" \
        --config "${config_file}"
done

# ============================================================================
# Upload results to S3
# ============================================================================

echo "Uploading results to S3..."
for source in "${SOURCES[@]}"; do
    output_dir="pretraining-data/sources/${source}_stack_edu_markdown_tagged_partitioned"
    local_dir="${LOCAL_DIR}/${output_dir}"
    s3_dir="${REMOTE_DIR}/${output_dir}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${source} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
