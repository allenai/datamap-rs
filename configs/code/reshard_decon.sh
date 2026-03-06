#!/bin/bash


input_dir="/mnt/raid0/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged_partitioned_decon"

output_dir="/mnt/raid0/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged_partitioned_decon_reshard"

cd $HOME/datamap-rs

for pl in $(ls --color=never $input_dir); do
    [[ "$pl" == *_reports ]] && continue

    echo "Processing $pl"
    cargo run --release -- reshard \
        --input-dir $input_dir/$pl \
        --max-lines 20000 \
        --output-dir $output_dir/$pl
done
