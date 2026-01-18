#!/usr/bin/env bash

BASE_DIR="/mnt/raid0"
INPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard"
OUTPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged"


for upper_lang in $(ls --color=never ${INPUT_DIR}); do
    lower_lang=$(echo ${upper_lang} | tr '[:upper:]' '[:lower:]')
    echo "Tagging ${upper_lang}..."
    cargo run --release map \
        --input-dir ${INPUT_DIR}/${upper_lang}/step_final/ \
        --output-dir ${OUTPUT_DIR}/${upper_lang}/ \
        --config configs/code/classifiers/${lower_lang}.yaml
done


for upper_lang in $(ls --color=never ${OUTPUT_DIR}); do
    echo "Sampling ${upper_lang}..."
    cargo run --release reservoir-sample \
    --input-dir ${OUTPUT_DIR}/${upper_lang}/step_final/ \
    --output-file ${OUTPUT_DIR}/${upper_lang}/code_quality_report.json \
    --key "metadata.code_quality.__label__pos" \
    --text-key text \
    --token-weighted
done
