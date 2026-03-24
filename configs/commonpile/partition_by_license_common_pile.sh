#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"

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

# Each entry is "base_dir/source_name" relative to REMOTE_DIR/LOCAL_DIR
ALL_SOURCES=(
    "pretraining-data/sources/common-pile_codetextish/v0/common-pile_github_archive_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_codetextish/v0/common-pile_stackexchange_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_codetextish/v0/common-pile_ubuntu_irc_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_biodiversity_heritage_library_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_libretexts_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_news_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_oercommons_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_pressbooks_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_public_domain_review_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_texbookish/v0/common-pile_youtube_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_wikish/v0/common-pile_wikimedia_filtered_decon_ngram_filtered"
    "pretraining-data/sources/common-pile_wikish/v0/common-pile_wikiteam_filtered_decon_ngram_filtered"
)

# Compute which sources this instance should process
# Each instance processes sources where (source_index % world_size) == rank
SOURCES=()
for i in "${!ALL_SOURCES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq ${RANK} ]; then
        SOURCES+=("${ALL_SOURCES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process ${#SOURCES[@]} sources:"
for s in "${SOURCES[@]}"; do echo "  ${s}"; done

# ============================================================================
# Setup environment
# ============================================================================

if [ ! -d ".venv" ]; then
    uv venv --python=3.12
fi

# ============================================================================
# Process sources: partition by license
# ============================================================================

for source_path in "${SOURCES[@]}"; do
    local_input_dir="${LOCAL_DIR}/${source_path}"
    local_output_dir="${LOCAL_DIR}/${source_path}_license_partitioned"

    remote_input_dir="${REMOTE_DIR}/${source_path}"
    remote_output_dir="${REMOTE_DIR}/${source_path}_license_partitioned"

    # download files if not present locally
    if [ ! -d "${local_input_dir}" ]; then
        echo "Downloading from ${remote_input_dir}..."
        mkdir -p "${local_input_dir}"
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    if [ ! -d "${local_input_dir}" ]; then
        echo "Input directory ${local_input_dir} not found... Skipping"
        continue
    fi

    remote_exists=false
    if s5cmd ls "${remote_output_dir}/*" >/dev/null 2>&1; then
        remote_exists=true
    fi

    if [ -d "${local_output_dir}" ] || [ "${remote_exists}" = true ]; then
        echo "Output already exists at ${local_output_dir} or ${remote_output_dir}... Skipping"
        continue
    fi

    echo "Partitioning ${source_path} by license..."
    uv run "${PARTITION_SCRIPT}" \
        --input-dir "${local_input_dir}" \
        --output-dir "${local_output_dir}"

    # Upload results to S3
    if [ -d "${local_output_dir}" ]; then
        echo "Uploading to ${remote_output_dir}..."
        s5cmd cp -sp "${local_output_dir}/*" "${remote_output_dir}/"
    fi
done

echo "Done!"
