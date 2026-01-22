#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
INPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_auto5"
OUTPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_auto5_filtered"


for language in $(ls --color=never "${LOCAL_DIR}/${INPUT_DIR}"); do
    echo "Filtering ${language}..."
    cargo run --release map \
        --input-dir "${LOCAL_DIR}/${INPUT_DIR}/${language}/step_final/" \
        --output-dir "${LOCAL_DIR}/${OUTPUT_DIR}/${language}/" \
        --config "${CONFIGS_DIR}/$(echo ${language} | tr '[:upper:]' '[:lower:]').yaml"
done
