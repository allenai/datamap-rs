#!/bin/bash


# Store the input argument
X=13


# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/output"


# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
cd ~/datamap-rs
git checkout main; git pull
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/sa_minlen500/annotated/shard_00${X}/*" "/mnt/raid0/input/"


# Step 2: Run the map operation
echo "Running sort/filter operations..."
cargo run --release -- map --input-dir /mnt/raid0/input/ --output-dir /mnt/raid0/output/ --config examples/all_dressed/fuzzy_suffarr.yaml


echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/output/step_final/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/sa_minlen500/filtered/"

echo "Processing complete for $X"