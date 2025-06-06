#!/bin/bash

set -ex

sudo dnf install python3 python3-pip

pip3 install uv

uv pip install dolma huggingface-hub

src=/mnt/raid0/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/data


huggingface-cli download allenai/dolma2-tokenizer --local-dir /mnt/raid0/tokenizer

# get the only directory in the src directory
dir=$(ls -d $src/*)

# the destination replace "/pretraining-data/sources" with "preprocessed"
dst=$(echo $dir | sed 's|/pretraining-data/sources/|/preprocessed/|' | sed 's|/data|/allenai/dolma2-tokenizer|')

# run it so that we run in parallel K = max_cores // max_cores_each
M=32
N=$(nproc)
K=$((N / M))


for y in $(ls -d $src/*); do
    # Wait if we've reached max concurrent jobs
    while (( $(jobs -r | wc -l) >= K )); do
        sleep 1.0
    done

    name=$(basename $y)

    # Run command in background
    uv run dolma tokens \
    --documents $y \
    --destination $dst/$name \
    --tokenizer.name_or_path /mnt/raid0/tokenizer/tokenizer.json \
    --tokenizer.eos_token_id 100257 \
    --tokenizer.pad_token_id 100277 \
    --no-tokenizer.segment_before_tokenization \
    --tokenizer.encode_special_tokens \
    --ring_size $M \
    --processes $M \
    --max_size 4_000_000_000 \
    --sample_ring_prop \
    --dtype 'uint32'
done

# Wait for all jobs to finish
wait

s5cmd cp -sp "$dst/*" "$(sed 's|/mnt/raid0/|s3://|g')/"
