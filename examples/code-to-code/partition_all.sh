#!/bin/bash

set -ex

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
    "Rust"
    "SQL"
    "Shell"
    "Swift"
    "TypeScript"
)

DRIVE="${DRIVE:-/mnt/raid0}"

remote_dest_dir="s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned"

source_dir="${DRIVE}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated/data"
output_dir="${DRIVE}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/data"
error_dir="${DRIVE}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_partitioned/error"

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
        mkdir -p $output_dir/$language
        mkdir -p $error_dir/$language
        cargo run --release -- map \
            --config examples/code-to-code/partition/$language.yaml \
            --input-dir $source_dir/$language/step_final \
            --output-dir $output_dir/$language \
            --err-dir $error_dir/$language

        s5cmd cp -sp "${output_dir}/${language}/*" "${remote_dest_dir}/data/${language}/"
        s5cmd cp -sp "${error_dir}/${language}/*" "${remote_dest_dir}/error/${language}/"
    else
        echo "Skipping ${languages[$i]} (index $i) - assigned to rank $((i % world_size))"
    fi
done
