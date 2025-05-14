#!/bin/bash
# get the directory of the script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# go to top directory with the script to download the models
current_dir=$(pwd)

# i/o locations
source_data="/mnt/raid0/ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii/documents"
output_data="/mnt/raid0/ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii_quality_filtered/documents"
# source_data="/mnt/raid0/smolpdf"
# output_data="/mnt/raid0/smolpdf_quality_filtered"



# location of the models
models_dir="/mnt/raid0/models"

mkdir -p $models_dir

# download facebook lid to this directory (only if not already present)
if [ ! -f "$models_dir/lid.176.bin" ]; then
    wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin -O $models_dir/lid.176.bin
fi


# download DCLM quality model (only if not already present)
if [ ! -f "$models_dir/openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train.bin" ]; then
    wget https://dolma-artifacts.org/fasttext_models/dclm-baseline-1.0/openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train.bin -O $models_dir/openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train.bin
fi


# download alex fasttext fineweb-edu (only if not already present)
if [ ! -f "$models_dir/fineweb_edu_gt2_bigram_200k.bin" ]; then
    s5cmd cp s3://ai2-llm/pretraining-data/sources/dclm/refinedweb/dolma_reformat/fineweb_edu_gt2_bigram_200k.bin $models_dir/fineweb_edu_gt2_bigram_200k.bin
fi

# move to top directory with the rust code
cd $script_dir/../../

# run the project
cargo run --release -- map --input-dir $source_data --output-dir $output_data --config $script_dir/config.yaml

# return to the directory the script was run from
cd $current_dir
