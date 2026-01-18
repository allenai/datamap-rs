#!/usr/bin/env bash

INPUT_DIR="/mnt/raid0/ai2-llm/classifiers/code-quality/data/the-stack-v2/spring2code_v2/minhash_v2_annotated/sample_1GB/countup_criteria_v2/gpt-5-mini/10k_trimmed"

OUTPUT_DIR="/mnt/raid0/ai2-llm/classifiers/code-quality/data-self-tagged/the-stack-v2/spring2code_v2/minhash_v2_annotated/sample_1GB/countup_criteria_v2/gpt-5-mini/10k_trimmed"

LANGUAGES=(
    "C"
    "C++"
    "C-Sharp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Python"
    "Ruby"
    "Rust"
    "Shell"
    "SQL"
    "Swift"
    "TypeScript"
)


for language in "${LANGUAGES[@]}"; do
    echo "Tagging ${language}..."
    cargo run --release map \
        --input-dir ${INPUT_DIR}/${language}/ \
        --output-dir ${OUTPUT_DIR}/${language}/ \
        --config configs/code/classifiers/$(echo ${language} | tr '[:upper:]' '[:lower:]').yaml
done

for language in "${LANGUAGES[@]}"; do
    echo "Sampling ${language}..."
    cargo run --release reservoir-sample \
    --input-dir ${OUTPUT_DIR}/${language}/step_final/ \
    --output-file ${OUTPUT_DIR}/${language}/report.json \
    --key "metadata.code_quality.__label__pos" \
    --text-key text \
    --token-weighted
done

# THRESHOLDS=(
#    "10.9"
#    "38.5"
#    "10.8"
#    "17.7"
#    "30.6"
#    "16.8"
#    "38.6"
#    "16.0"
#    "36.9"
#    "21.6"
#    "40.7"
#    "61.9"
#    "51.6"
#    "32.3"
#    "20.7"
# )
