#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
INPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged_partitioned"
OUTPUT_DIR=$(echo "${INPUT_DIR}" | sed -e 's|pretraining-data/sources|preprocessed|g')

FIELD_ID=${1:-id}
EXTENSION=${2:-*}
TOKENIZER_NAME=${3:-allenai/dolma2-tokenizer}

ALL_LANGUAGES=(
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

if [ ! -d ".venv" ]; then
    # setting up virtual environment
    uv venv
fi

# installing dolma
uv pip install dolma backports-zstd backports-weakref

# downloading dolma2-tokenizer
uv run --with=huggingface-hub \
    hf download ${TOKENIZER_NAME} \
    --local-dir ${LOCAL_DIR}/huggingface/${TOKENIZER_NAME}

for language in "${ALL_LANGUAGES[@]}"; do
    local_input_dir="${LOCAL_DIR}/${INPUT_DIR}/${language}"
    local_output_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"

    if [ ! -d "${local_input_dir}" ]; then
        echo "Input directory ${local_input_dir} not found... Skipping ${language}"
        continue
    fi

    for step_dir in $(ls --color=never "${local_input_dir}"); do
        # tokenizing the language
        uv run dolma tokens \
            --documents "${local_input_dir}/${step_dir}/${EXTENSION}" \
            --destination "${local_output_dir}/${step_dir}/${TOKENIZER_NAME}" \
            --tokenizer.name_or_path ${TOKENIZER_NAME} \
            --tokenizer.eos_token_id 100257 \
            --tokenizer.pad_token_id 100277 \
            --fields.id_field_name ${FIELD_ID} \
            --no-tokenizer.segment_before_tokenization \
            --tokenizer.encode_special_tokens \
            --processes $(python3 -c "import multiprocessing; print(multiprocessing.cpu_count())") \
            --max_size 4_000_000_000 \
            --sample_ring_prop \
            --dtype uint32
    done
done

# ============================================================================
# Upload results to S3
# ============================================================================

echo "Uploading results to S3..."
for language in "${ALL_LANGUAGES[@]}"; do
    local_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"
    s3_dir="${REMOTE_DIR}/${OUTPUT_DIR}/${language}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${language} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
