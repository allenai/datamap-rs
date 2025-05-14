#!/bin/bash
# get the directory of the script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# go to top directory with the script to download the models
current_dir=$(pwd)
cd $script_dir

# assuming locations on aws
source_data="/mnt/raid0/ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii"
output_data="/mnt/raid0/ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii_quality_filtered"

# download facebook lid to this directory (only if not already present)
if [ ! -f "lid.176.bin" ]; then
    wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
fi


# download DCLM quality model (only if not already present)
if [ ! -f "openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train.bin" ]; then
    wget https://dolma-artifacts.org/fasttext_models/dclm-baseline-1.0/openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train.bin
fi


# download alex fasttext fineweb-edu (only if not already present)
if [ ! -f "fineweb_edu_gt2_bigram_200k.bin" ]; then
    s5cmd cp s3://ai2-llm/pretraining-data/sources/dclm/refinedweb/dolma_reformat/fineweb_edu_gt2_bigram_200k.bin .
fi

# move to top directory with the rust code
cd $current_dir/../../

# run the project
cargo run --release -- datamap map --input_dir $source_data --output_dir $output_data --config $script_dir/config.yaml

# return to the directory the script was run from
cd $current_dir
