#!/usr/bin/env bash

set -ex

BASE_DIR="/mnt/raid0"
TOKENIZER_NAME="allenai/dolma2-tokenizer"
INPUT_DIR="${BASE_DIR}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_filtered_fixed"
OUTPUT_DIR="${BASE_DIR}/ai2-llm/preprocessed/the-stack-v2/spring2code_v2/minhash_v2_annotated_reshard_qc_tagged_filtered_fixed"
PROGRAMMING_LANGUAGE=$1

if [ -z "${PROGRAMMING_LANGUAGE}" ]; then
    echo "Programming language is required"
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

# tokenizing the language
uv run dolma tokens \
    --documents "${INPUT_DIR}/${PROGRAMMING_LANGUAGE}/step_final/*" \
    --destination "${OUTPUT_DIR}/${PROGRAMMING_LANGUAGE}/${TOKENIZER_NAME}" \
    --tokenizer.name_or_path ${TOKENIZER_NAME} \
    --tokenizer.eos_token_id 100257 \
    --tokenizer.pad_token_id 100277 \
    --fields.id_field_name blob_id \
    --no-tokenizer.segment_before_tokenization \
    --tokenizer.encode_special_tokens \
    --processes $(python3 -c "import multiprocessing; print(multiprocessing.cpu_count())") \
    --max_size 4_000_000_000 \
    --sample_ring_prop \
    --dtype uint32
