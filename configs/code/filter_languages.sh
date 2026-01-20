#!/usr/bin/env bash

INPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_fixed"
OUTPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_filtered_fixed"


set -exou


for language in $(ls --color=never ${INPUT_DIR}); do
    echo "Filtering ${language}..."
    cargo run --release map \
        --input-dir ${INPUT_DIR}/${language}/step_final/ \
        --output-dir ${OUTPUT_DIR}/${language}/ \
        --config "configs/code/filters/$(echo ${language} | tr '[:upper:]' '[:lower:]').yaml"
done
