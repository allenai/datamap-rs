#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
BASE_DIR="pretraining-data/sources"

EXTENSION="*.gz"
TOKENIZER_NAME="allenai/dolma2-tokenizer"

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
echo "Instance Index: ${INSTANCE_IDX} (Rank: ${RANK})"

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

ALL_SOURCES=(
    "finemath/finemath-3plus-decon-sparkle_ngram_filtered_minhash_deduped"
    "proof-pile-2/v0_decon_v1/documents/rpj-proofpile-arxiv/train_minhash_deduped_ngram_filtered"
)

SOURCES=()
for i in "${!ALL_SOURCES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq "${RANK}" ]; then
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

uv pip install dolma backports-zstd backports-weakref

uv run --with=huggingface-hub \
    hf download ${TOKENIZER_NAME} \
    --local-dir ${LOCAL_DIR}/huggingface/${TOKENIZER_NAME}

# setup resharder
current_dir=$(pwd)
cd $HOME
if [ ! -d "reshard-tokenized" ]; then
    git clone https://github.com/soldni/reshard-tokenized.git
fi

cd reshard-tokenized
git pull
cargo build --release
RESHARDER_BIN="$(pwd)/target/release/reshard-tokenized"
cd $current_dir

# ============================================================================
# Process sources: tokenization
# ============================================================================

for source in "${SOURCES[@]}"; do
    input_dir="${BASE_DIR}/${source}"
    output_dir=$(echo "${input_dir}" | sed 's|^pretraining-data/sources|preprocessed|')

    local_input_dir="${LOCAL_DIR}/${input_dir}"
    local_temp_dir="${LOCAL_DIR}/${output_dir}_temp"
    local_output_dir="${LOCAL_DIR}/${output_dir}/${TOKENIZER_NAME}/0000"

    # download files if not present locally
    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${input_dir}"
        echo "Downloading ${source} from ${remote_input_dir}..."
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    if [ ! -d "${local_input_dir}" ]; then
        echo "Input directory ${local_input_dir} not found... Skipping ${source}"
        continue
    fi

    # tokenizing the source
    uv run dolma tokens \
        --documents "${local_input_dir}/${EXTENSION}" \
        --destination "${local_temp_dir}" \
        --tokenizer.name_or_path ${TOKENIZER_NAME} \
        --tokenizer.eos_token_id 100257 \
        --tokenizer.pad_token_id 100277 \
        --no-tokenizer.segment_before_tokenization \
        --tokenizer.encode_special_tokens \
        --processes $(nproc) \
        --max_size 4_000_000_000 \
        --sample_ring_prop \
        --dtype uint32

    # reshard to just one file
    $RESHARDER_BIN \
        --input-path "${local_temp_dir}" \
        --output-path "${local_output_dir}" \
        --num-files 1

    rm -rf $local_temp_dir

    # upload
    output_dir=$(echo "${BASE_DIR}/${source}" | sed 's|^pretraining-data/sources|preprocessed|')
    local_dir="${LOCAL_DIR}/${output_dir}"
    s3_dir="${REMOTE_DIR}/${output_dir}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${source} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
