#!/usr/bin/env bash

set -ex

REMOTE_PREFIX="s3:/"
LOCAL_PREFIX="/mnt/raid0"


DATA_PATH="ai2-llm/pretraining-data/sources/tokyotech-llm/swallowcode/scor_final_data-decon-sparkle-motion-with-ids-modelnamefilter2"
FILTERED_PATH="${DATA_PATH}_ngram_gzip_cleanup"
TOKENIZER_NAME="allenai/dolma2-tokenizer"

mkdir -p tokenizers
if [ ! -f "tokenizers/deepseek_v2.json" ]; then
    curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json
fi


# Step 1: Download data if not already downloaded
if [ ! -d "${LOCAL_PREFIX}/${DATA_PATH}" ]; then
    s5cmd cp -sp "${REMOTE_PREFIX}/${DATA_PATH}/*" "${LOCAL_PREFIX}/${DATA_PATH}/"
fi


# Step 2: run datamap-rs
if [ ! -d "${LOCAL_PREFIX}/${FILTERED_PATH}" ]; then
    config_path="$(dirname $(dirname $0))/filters/$(basename $0 | sed 's/.sh/.yaml/g')"
    cargo run --release map \
        --config "${config_path}" \
        --input-dir "${LOCAL_PREFIX}/${DATA_PATH}" \
        --output-dir "${LOCAL_PREFIX}/${FILTERED_PATH}"
fi

exit

# Step 3: Tokenize data
TOKENIZED_PATH="${LOCAL_PREFIX}/$(echo $FILTERED_PATH | sed 's|pretraining-data/sources|prepocessed|g')/${TOKENIZER_NAME}"

if [ -d "${TOKENIZED_PATH}" ]; then
    echo "Data already tokenized"
    exit 0
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
    --local-dir ${LOCAL_PREFIX}/huggingface/${TOKENIZER_NAME}

# tokenizing the language
uv run dolma tokens \
    --documents "${LOCAL_PREFIX}/${FILTERED_PATH}/step_final" \
    --destination "${TOKENIZED_PATH}" \
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
