#!/usr/bin/env bash

BASE_DIR="/mnt/raid0"
INPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard"
OUTPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged"


LANGUAGES=(
    "Python"
    "C"
    "C++"
    "C-Sharp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Ruby"
    "Rust"
    "Shell"
    "SQL"
    "Swift"
    "TypeScript"
)


for language in "${LANGUAGES[@]}"; do
    if [ "${language}" == "C++" ]; then
        config_file="configs/code/classifiers/cpp.yaml"
    elif [ "${language}" == "C-Sharp" ]; then
        config_file="configs/code/classifiers/csharp.yaml"
    else
        config_file="configs/code/classifiers/$(echo ${language} | tr '[:upper:]' '[:lower:]').yaml"
    fi

    input_dir="${INPUT_DIR}/${language}/step_final/"
    output_dir="${OUTPUT_DIR}/${language}/"

    if [ -d "${output_dir}" ]; then
        echo "Output directory ${output_dir} already exists"
        continue
    fi

    echo "Tagging ${language} with config ${config_file}..."
    cargo run --release map --input-dir ${input_dir} --output-dir ${output_dir} --config ${config_file}
done


for language in "${LANGUAGES[@]}"; do
    input_dir="${OUTPUT_DIR}/${language}/step_final/"
    output_file="${OUTPUT_DIR}/${language}/code_quality_report.json"

    if [ -f "${output_file}" ]; then
        echo "Output file ${output_file} already exists"
        continue
    fi

    echo "Sampling ${language}..."
    cargo run --release reservoir-sample --input-dir ${input_dir} --output-file ${output_file} \
    --key "metadata.code_quality.__label__pos" --text-key text --token-weighted --reservoir-size 100000
done
