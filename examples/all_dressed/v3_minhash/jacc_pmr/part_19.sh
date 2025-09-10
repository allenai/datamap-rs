#!/bin/bash


# Store the input argument
X=19






# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/output"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
cd ~/datamap-rs
git checkout minhash; git pull
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/minhash/param_26_11/sorted_v2/${X}/*" "/mnt/raid0/input/"


# Step 2: Run the map operation
echo "Running sort/filter operations..."

cargo run --release -- jaccard-filter --input-dir /mnt/raid0/input --output-dir /mnt/raid0/output  --config examples/all_dressed/minhash_groupsort.yaml  > "/mnt/raid0/jacc_filter_${X}.log"



echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/output/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/minhash/param_26_11/jacc_filter/${X}/"
s5cmd cp -sp "/mnt/raid0/*.log" s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/logs/

echo "Processing complete for $X"