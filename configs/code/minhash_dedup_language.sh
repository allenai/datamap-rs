#!/usr/bin/env bash

# Downloads data for a single programming language from S3, runs MinHash
# fuzzy deduplication using duplodocus, and uploads the result.
#
# Usage:
#   ./configs/code/minhash_dedup_language.sh <Language>
#
# Example:
#   ./configs/code/minhash_dedup_language.sh Python
#
# Environment variables:
#   LOCAL_DIR         - Local scratch directory (default: /mnt/raid0/ai2-llm)
#   DUPLODOCUS_BIN    - Path to duplodocus binary (default: $HOME/duplodocus/target/release/duplodocus)

set -euox pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <Language>"
    exit 1
fi

LANGUAGE="$1"

LOCAL_DIR="${LOCAL_DIR:-/mnt/raid0/ai2-llm}"
DUPLODOCUS_BIN="${DUPLODOCUS_BIN:-${HOME}/duplodocus/target/release/duplodocus}"

S3_SOURCE="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/data/${LANGUAGE}"
S3_OUTPUT="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_2026/${LANGUAGE}"

LOCAL_INPUT="${LOCAL_DIR}/pretraining-data/sources/the-stack-v2/spring2code_v2/data/${LANGUAGE}"
LOCAL_OUTPUT="${LOCAL_DIR}/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_2026/${LANGUAGE}"
LOCAL_STORAGE="${LOCAL_DIR}/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_2026_work/${LANGUAGE}"

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
# Step 1: Download data from S3
# ============================================================================

if [ ! -d "${LOCAL_INPUT}" ] || [ -z "$(ls -A "${LOCAL_INPUT}" 2>/dev/null)" ]; then
    echo "Downloading ${LANGUAGE} data from S3..."
    mkdir -p "${LOCAL_INPUT}"
    s5cmd cp -sp "${S3_SOURCE}/*" "${LOCAL_INPUT}/"
else
    echo "Using existing data at ${LOCAL_INPUT}"
fi

# ============================================================================
# Step 2: Run MinHash deduplication
# ============================================================================

echo "Running MinHash deduplication for ${LANGUAGE}..."
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

# ============================================================================
# Step 3: Upload results to S3
# ============================================================================

echo "Uploading results to ${S3_OUTPUT}..."
s5cmd cp -sp "${LOCAL_OUTPUT}/*" "${S3_OUTPUT}/step_final/"

echo "Done! Results at ${S3_OUTPUT}/step_final/"
