#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
INPUT_BASE_DIR="pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned_decon_sa10x/s2orcforolmo_nogpl_ngram_filtered"
OUTPUT_BASE_DIR="pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned_decon_sa10x/s2orcforolmo_nogpl_ngram_filtered_license_partioned"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARTITION_SCRIPT="${SCRIPT_DIR}/partition_by_license.py"

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
    "__label__agricultural-and-food-sciences"
    "__label__art"
    "__label__biology"
    "__label__business"
    "__label__chemistry"
    "__label__computer-science"
    "__label__economics"
    "__label__education"
    "__label__engineering"
    "__label__environmental-science"
    "__label__geography"
    "__label__geology"
    "__label__history"
    "__label__law"
    "__label__linguistics"
    "__label__materials-science"
    "__label__mathematics"
    "__label__medicine"
    "__label__philosophy"
    "__label__physics"
    "__label__political-science"
    "__label__psychology"
    "__label__sociology"
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
# Setup environment
# ============================================================================

if [ ! -d ".venv" ]; then
    uv venv --python=3.12
fi

# ============================================================================
# Process sources: partition by license
# ============================================================================

for source in "${SOURCES[@]}"; do
    local_input_dir="${LOCAL_DIR}/${INPUT_BASE_DIR}/${source}"
    local_output_dir="${LOCAL_DIR}/${OUTPUT_BASE_DIR}/${source}"

    remote_input_dir="${REMOTE_DIR}/${INPUT_BASE_DIR}/${source}"
    remote_output_dir="${REMOTE_DIR}/${OUTPUT_BASE_DIR}/${source}"

    # download files if not present locally
    if [ ! -d "${local_input_dir}" ]; then
        echo "Downloading ${source} from ${remote_input_dir}..."
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    if [ ! -d "${local_input_dir}" ]; then
        echo "Input directory ${local_input_dir} not found... Skipping ${source}"
        continue
    fi

    for step_dir in $(ls --color=never "${local_input_dir}"); do
        if [[ "${step_dir}" != qual_00* ]]; then
            echo "Skipping ${step_dir} as it does not start with qual_00"
            continue
        fi

        current_output="${local_output_dir}/${step_dir}"
        current_remote_output="${remote_output_dir}/${step_dir}"
        remote_exists=false

        if s5cmd ls "${current_remote_output}/*" >/dev/null 2>&1; then
            remote_exists=true
        fi

        if [ -d "${current_output}" ] || [ "${remote_exists}" = true ]; then
            echo "Output already exists at ${current_output} or ${current_remote_output}... Skipping ${source}/${step_dir}"
            continue
        fi

        echo "Partitioning ${source}/${step_dir} by license..."
        uv run "${PARTITION_SCRIPT}" \
            --input-dir "${local_input_dir}/${step_dir}" \
            --output-dir "${current_output}"
    done

    # Upload results to S3
    if [ -d "${local_output_dir}" ]; then
        echo "Uploading ${source} to ${remote_output_dir}..."
        s5cmd cp -sp "${local_output_dir}/*" "${remote_output_dir}/"
    fi
done

echo "Done!"
