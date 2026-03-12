#!/usr/bin/env bash

# Runs MinHash fuzzy deduplication on leftover sources using duplodocus,
# then applies ngram repetition filtering (unless already ngram-filtered).
#
# Usage:
#   ./configs/commonpile/leftover_minhash.sh
#
# Environment variables:
#   LOCAL_DIR         - Local scratch directory (default: /mnt/raid0/ai2-llm)
#   DUPLODOCUS_BIN    - Path to duplodocus binary (default: $HOME/duplodocus/target/release/duplodocus)

set -euox pipefail

LOCAL_DIR="${LOCAL_DIR:-/mnt/raid0/ai2-llm}"
DUPLODOCUS_BIN="${DUPLODOCUS_BIN:-${HOME}/duplodocus/target/release/duplodocus}"

REMOTE_DIR="s3://ai2-llm"
BASE_SOURCES="pretraining-data/sources"

ALL_SOURCES=(
    "finemath/finemath-3plus-decon-sparkle_ngram_filtered"
    "proof-pile-2/v0_decon_v1/documents/rpj-proofpile-arxiv/train"
)

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
# Compute subset for this instance
# ============================================================================

SOURCES=()
for i in "${!ALL_SOURCES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq "${RANK}" ]; then
        SOURCES+=("${ALL_SOURCES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${SOURCES[*]}"

# ============================================================================
# Build ngram filter config
# ============================================================================

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

# ============================================================================
# Ensure datamap-rs is available
# ============================================================================

current_dir=$(pwd)
cd "${HOME}/datamap-rs"
git pull
cd "${current_dir}"

# ============================================================================
# Ensure duplodocus is available (clone & build if needed)
# ============================================================================

if [ ! -f "${DUPLODOCUS_BIN}" ]; then
    DUPLODOCUS_DIR="$(dirname "$(dirname "$(dirname "${DUPLODOCUS_BIN}")")")"
    echo "duplodocus binary not found at ${DUPLODOCUS_BIN}, building..."

    if ! command -v cargo &> /dev/null; then
        echo "Installing Rust..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "${HOME}/.cargo/env"
    fi

    if [ ! -d "${DUPLODOCUS_DIR}/.git" ]; then
        echo "Cloning duplodocus..."
        git clone https://github.com/allenai/duplodocus.git "${DUPLODOCUS_DIR}"
    fi

    echo "Building duplodocus..."
    cargo build --release --manifest-path "${DUPLODOCUS_DIR}/Cargo.toml"

    if [ ! -f "${DUPLODOCUS_BIN}" ]; then
        echo "ERROR: build succeeded but binary not found at ${DUPLODOCUS_BIN}"
        exit 1
    fi
fi

# ============================================================================
# Process each source
# ============================================================================

for source in "${SOURCES[@]}"; do
    S3_SOURCE="${REMOTE_DIR}/${BASE_SOURCES}/${source}"
    S3_OUTPUT="${REMOTE_DIR}/${BASE_SOURCES}/${source}_minhash_deduped"

    LOCAL_INPUT="${LOCAL_DIR}/${BASE_SOURCES}/${source}"
    LOCAL_OUTPUT="${LOCAL_DIR}/${BASE_SOURCES}/${source}_minhash_deduped"
    LOCAL_STORAGE="${LOCAL_DIR}/${BASE_SOURCES}/${source}_minhash_work"

    # ---- Step 1: Download data from S3 ----

    if [ ! -d "${LOCAL_INPUT}" ] || [ -z "$(ls -A "${LOCAL_INPUT}" 2>/dev/null)" ]; then
        echo "Downloading ${source} from S3..."
        mkdir -p "${LOCAL_INPUT}"
        s5cmd cp -sp "${S3_SOURCE}/*" "${LOCAL_INPUT}/"
    else
        echo "Using existing data at ${LOCAL_INPUT}"
    fi

    # ---- Step 2: Run MinHash deduplication ----

    echo "Running MinHash deduplication for ${source}..."
    mkdir -p "${LOCAL_OUTPUT}" "${LOCAL_STORAGE}"

    "${DUPLODOCUS_BIN}" minhash-memory \
        --input-dir "${LOCAL_INPUT}" \
        --storage-dir "${LOCAL_STORAGE}" \
        --output-dir "${LOCAL_OUTPUT}" \
        --text-key "text" \
        --tokenizer cl100k \
        --num-buckets 20 \
        --bucket-size 5 \
        --ngram-size 5 \
        --remove-duplicates true \
        --cleanup-storage

    # ---- Step 3: Ngram filter (if not already ngram-filtered) ----

    if [[ "${source}" == *"ngram_filtered"* ]]; then
        echo "Source already ngram-filtered, skipping ngram filter for ${source}"
        FINAL_LOCAL="${LOCAL_OUTPUT}"
        S3_FINAL="${S3_OUTPUT}"
    else
        echo "Running ngram filter for ${source}..."
        LOCAL_NGRAM="${LOCAL_DIR}/${BASE_SOURCES}/${source}_minhash_deduped_ngram_filtered"
        S3_FINAL="${REMOTE_DIR}/${BASE_SOURCES}/${source}_minhash_deduped_ngram_filtered"
        mkdir -p "${LOCAL_NGRAM}"

        cd "${HOME}/datamap-rs"
        cargo run --release map \
            --input-dir "${LOCAL_OUTPUT}" \
            --output-dir "${LOCAL_NGRAM}" \
            --config "${HOME}/ngram_filter.yaml" | tee -a "${LOCAL_NGRAM}/datamap.log"
        cd "${current_dir}"

        FINAL_LOCAL="${LOCAL_NGRAM}"
    fi

    # ---- Step 4: Upload final results to S3 ----

    echo "Uploading results to ${S3_FINAL}..."
    if [ -d "${FINAL_LOCAL}/step_final" ]; then
        s5cmd cp -sp "${FINAL_LOCAL}/step_final/*" "${S3_FINAL}/"
    else
        s5cmd cp -sp "${FINAL_LOCAL}/*" "${S3_FINAL}/"
    fi
    if [ -f "${FINAL_LOCAL}/datamap.log" ]; then
        s5cmd cp -sp "${FINAL_LOCAL}/datamap.log" "${S3_FINAL}/datamap.log"
    fi

    echo "Done with ${source}!"
done

echo "All done!"
