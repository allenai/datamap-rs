#!/usr/bin/env bash

set -ex


INPUT_DIR=$1
FIELD_ID=${2:-id}
EXTENSION=${3:-*}
TOKENIZER_NAME=${4:-allenai/dolma2-tokenizer}
OUTPUT_DIR=$(echo $INPUT_DIR | sed -e 's|pretraining-data/sources|preprocessed|g')
BASE_DIR=${5:-/mnt/raid0}

if [ ! -d "${INPUT_DIR}" ]; then
    echo "Valid input directory is required"
    exit 1
fi

if [ ! -d ".venv" ]; then
    # setting up virtual environment
    uv venv
fi

# installing dolma
uv pip install dolma backports-zstd backports-weakref

# downloading dolma2-tokenizer
uv run --with=huggingface-hub \
    hf download ${TOKENIZER_NAME} \
    --local-dir ${BASE_DIR}/huggingface/${TOKENIZER_NAME}


for step_dir in $(ls --color=never "${INPUT_DIR}")
do
    # tokenizing the language
    uv run dolma tokens \
        --documents "${INPUT_DIR}/${step_dir}/${EXTENSION}" \
        --destination "${OUTPUT_DIR}/${step_dir}/${TOKENIZER_NAME}" \
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
