#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR=${REMOTE_DIR:-"s3://ai2-llm"}
LOCAL_DIR=${LOCAL_DIR:-"/mnt/raid0/ai2-llm"}

USE_S3=1
if [ -z "${REMOTE_DIR}" ]; then
    USE_S3=0
    echo "REMOTE_DIR is empty; S3 download/upload disabled"
fi

# ============================================================================
# Get instance rank and world size from EC2 metadata
# ============================================================================

INSTANCE_ID=$(ec2-metadata --instance-id | grep -oP 'instance-id: \K(i-[a-f0-9]+)')
echo "Instance ID: ${INSTANCE_ID}"

INSTANCE_NAME=$(aws ec2 describe-tags \
    --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=Name" \
    --query "Tags[0].Value" --output text)
echo "Instance Name: ${INSTANCE_NAME}"

GROUP_NAME=$(echo "${INSTANCE_NAME}" | sed 's/-[0-9]\{4\}$//')
INSTANCE_IDX=$(echo "${INSTANCE_NAME}" | grep -oP '[0-9]{4}$')
RANK=$((10#${INSTANCE_IDX}))
echo "Group Name: ${GROUP_NAME}"
echo "Instance Index: ${INSTANCE_IDX} (Raw Rank: ${RANK})"

WORLD_SIZE=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${GROUP_NAME}-*" "Name=instance-state-name,Values=running" \
    --query "Reservations[*].Instances[*].InstanceId" --output text | wc -w)
echo "World Size: ${WORLD_SIZE}"

if [ "${WORLD_SIZE}" -eq 0 ]; then
    echo "No running instances found for group ${GROUP_NAME}"
    exit 1
fi

if [ "${WORLD_SIZE}" -eq 1 ]; then
    RANK=0
    echo "Single instance detected, setting RANK to 0"
else
    RANK=$((RANK % WORLD_SIZE))
    echo "Normalized Rank: ${RANK}"
fi

# ============================================================================
# Define all sources and compute subset for this instance
# ============================================================================

BASE_SOURCES="pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned_decon/s2orcforolmo"
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

SOURCES=()
for i in "${!ALL_SOURCES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq "${RANK}" ]; then
        SOURCES+=("${ALL_SOURCES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${SOURCES[*]}"

# ============================================================================
# Make config file
# ============================================================================

# Start building the config file with gzip quality guards.
cat > "${HOME}/ngram_filter.yaml" << EOF
name: ngram_filter
text_field: text
pipeline:
  - name: ngram_repetition_filter
    kwargs:
      rep_count: 128
      tokenizer_name: cl100k
      skip_offsets: true
EOF


# switch dir
cd $HOME/datamap-rs
git pull


# ============================================================================
# Process sources: filtering
# ============================================================================

for source in "${SOURCES[@]}"; do

    input_dir="${BASE_SOURCES}/${source}"
    output_dir="${BASE_SOURCES}_ngram_filtered/${source}"

    local_input_dir="${LOCAL_DIR}/${input_dir}"
    local_output_dir="${LOCAL_DIR}/${output_dir}"

    if [ -d "${local_output_dir}" ]; then
        echo "Output directory ${local_output_dir} already exists"
        continue
    fi

    if [ ! -d "${local_input_dir}" ]; then
        if [ "${USE_S3}" -eq 0 ]; then
            echo "Input directory ${local_input_dir} not found and REMOTE_DIR is empty... Skipping ${source}"
            continue
        fi

        remote_input_dir="${REMOTE_DIR}/${input_dir}"
        mkdir -p "${local_input_dir}"
        if ! s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"; then
            echo "Failed to download ${source} from ${remote_input_dir}... Skipping"
            continue
        fi
    fi

    # follow step_final directories to find the actual input
    while [ -d "${local_input_dir}/step_final" ]; do
        local_input_dir="${local_input_dir}/step_final"
    done

    for partition in $(ls --color=never "${local_input_dir}"); do
        if [[ ! "${partition}" =~ ^qual_([0-9]{4})$ ]]; then
            continue
        fi

        echo "Filtering ${source}/${partition} for ngrams..."
        partition_input_dir="${local_input_dir}/${partition}"
        partition_output_dir="${local_output_dir}/${partition}"
        mkdir -p $partition_output_dir

        cargo run --release map \
            --input-dir "${partition_input_dir}" \
            --output-dir "${partition_output_dir}" \
            --config "${HOME}/ngram_filter.yaml" | tee -a ${partition_output_dir}/datamap.log

        if [ "${USE_S3}" -eq 1 ]; then
            s3_output_dir="${REMOTE_DIR}/${output_dir}/${partition}"
            echo "Uploading ${source}/${partition} to ${s3_output_dir}..."
            s5cmd cp -sp "${partition_output_dir}/step_final/*" "${s3_output_dir}/"
            s5cmd cp -sp "${partition_output_dir}/datamap.log" "${s3_output_dir}/datamap.log"
        else
            echo "Skipping upload for ${source}/${partition} because REMOTE_DIR is empty"
        fi
    done
done

echo "Done!"
