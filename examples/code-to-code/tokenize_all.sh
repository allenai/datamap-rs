#!/bin/bash


languages=(
    "C"
    "C++"
    "C-Sharp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Python"
    # "Ruby"
    "Rust"
    "SQL"
    "Shell"
    "Swift"
    "TypeScript"
)


DRIVE="${DRIVE:-/mnt/raid0}"
tokenizer="allenai/dolma2-tokenizer"

remote_src_dir="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/pruned"
remote_dst_dir="s3://ai2-llm/preprocessed/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/pruned/${tokenizer}"

local_src_dir="${DRIVE}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/pruned"
local_dst_dir="${DRIVE}/ai2-llm/preprocessed/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/pruned/${tokenizer}"


local_tokenizer_dir="${DRIVE}/${tokenizer}"

# cache the tokenizer
uv run huggingface-cli download ${tokenizer} --local-dir ${local_tokenizer_dir}

get_instance_rank () {
    instance_id=$(ec2-metadata --instance-id | cut -d ' ' -f 2)
    instance_name=$(aws ec2 describe-instances --instance-ids $instance_id --query 'Reservations[0].Instances[0].Tags[?Key==`Name`].Value' --output text)
    instance_rank=$(echo $instance_name | rev | cut -d '-' -f 1 | rev)
    echo $instance_rank
}

get_world_size () {
    instance_id=$(ec2-metadata --instance-id | cut -d ' ' -f 2)
    project_name=$(aws ec2 describe-instances --instance-ids $instance_id --query 'Reservations[0].Instances[0].Tags[?Key==`Project`].Value' --output text)
    world_size=$(aws ec2 describe-instances \
        --filters "Name=tag:Project,Values=${project_name}" "Name=instance-state-name,Values=running" \
        --query 'length(Reservations[*].Instances[*])' \
        --output text)
    echo $world_size
}

# Get instance rank and world size
instance_rank=$(get_instance_rank)
world_size=$(get_world_size)

echo "Instance rank: $instance_rank, World size: $world_size"

# Process languages based on rank
for i in "${!languages[@]}"; do
    # Check if this language index should be processed by this instance
    if [ "$((i % world_size))" -eq "$instance_rank" ]; then
        language="${languages[$i]}"

        echo "Running $language (index $i)"

        if [ ! -d "${local_src_dir}/${language}" ]; then
            s5cmd cp -sp "${remote_src_dir}/${language}/*" "${local_src_dir}/${language}/"
        fi

        for partition in $(ls --color=never ${local_src_dir}/${language}); do
            # Run command in background
            uv run dolma tokens \
                --documents "${local_src_dir}/${language}/${partition}/*.jsonl.zst" \
                --destination "${local_dst_dir}/${language}/${partition}" \
                --tokenizer.name_or_path ${local_tokenizer_dir}/tokenizer.json \
                --tokenizer.eos_token_id 100257 \
                --tokenizer.pad_token_id 100277 \
                --no-tokenizer.segment_before_tokenization \
                --tokenizer.encode_special_tokens \
                --ring_size 32 \
                --processes $(nproc) \
                --fields.id_field_name "" \
                --max_size 4_000_000_000 \
                --sample_ring_prop \
                --dtype 'uint32'
        done

        s5cmd cp -sp "${local_dst_dir}/${language}/*" "${remote_dst_dir}/${language}/"

    else
        echo "Skipping ${languages[$i]} (index $i) - assigned to rank $((i % world_size))"
    fi
done
