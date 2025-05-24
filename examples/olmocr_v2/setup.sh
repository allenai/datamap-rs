#!/bin/bash

set -ex

# get the directory of the script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# go to top directory with the script to download the models
current_dir=$(pwd)

# download the source data
remote_source_prefix="s3://ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_30b/documents"
local_source_prefix=$(echo $remote_source_prefix | sed 's|^s3://|/mnt/raid0/|g')
local_destination_prefix=$(echo $local_source_prefix | sed 's|/documents|_no_refs|g')

if [ ! -d $local_source_prefix ]; then
    s5cmd cp -sp "${remote_source_prefix}/*" "${local_source_prefix}/"
fi

model_name="test_pdfs_references_fasttext_model_v2_pretok_wn3_ws10_lr05_e20_short"

# location of the models
models_dir="/mnt/raid0/models"
mkdir -p $models_dir

# download model if not already present
if [ ! -f "$models_dir/$model_name.bin" ]; then
    s5cmd cp -sp s3://ai2-llm/models/fasttext_reference_classifiers/$model_name.bin $models_dir/$model_name.bin
fi

# move to top directory with the rust code
cd $script_dir/../../

# run the project
cargo run --release -- map --input-dir $source_data --output-dir $output_data --config $script_dir/config.yaml

# return to the directory the script was run from
cd $current_dir

remote_destination_prefix=$(echo $local_destination_prefix | sed 's|^/mnt/raid0/|s3://|g')
s5cmd cp -sp "${local_destination_prefix}/step_final/*" "${remote_destination_prefix}/"
