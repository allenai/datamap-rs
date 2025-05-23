#!/bin/bash

set -ex

# get the directory of the script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# go to top directory with the script to download the models
current_dir=$(pwd)

# i/o locations
# source_data="tmp/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_30b/documents"
# output_data="tmp/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_30b_filtered/documents"
source_data="tmp/s2pdf_subset"
output_data="tmp/s2pdf_subset_filtered"
model_name="test_pdfs_references_fasttext_model_v2_pretok_wn3_ws10_lr05_e20_short"
# source_data="/mnt/raid0/smolpdf"
# output_data="/mnt/raid0/smolpdf_quality_filtered"



# location of the models
# models_dir="/mnt/raid0/models"
models_dir="tmp/models"


mkdir -p $models_dir

# download alex fasttext fineweb-edu (only if not already present)
if [ ! -f "$models_dir/$model_name.bin" ]; then
    s5cmd cp -sp s3://ai2-llm/models/fasttext_reference_classifiers/$model_name.bin $models_dir/$model_name.bin
fi

# move to top directory with the rust code
cd $script_dir/../../

# run the project
cargo run --release -- map --input-dir $source_data --output-dir $output_data --config $script_dir/config.yaml

# return to the directory the script was run from
cd $current_dir
