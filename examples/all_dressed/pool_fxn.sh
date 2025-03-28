#!/bin/bash

# Check if an argument was provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <directory-name>"
    echo "Example: $0 CC-MAIN-2023-06"
    echo "See one of the folders in s3://ai2-oe-data/contrib/datacomp/DCLM-pool/"
    exit 1
fi

# Store the input argument
X=$1
echo "Processing directory: $X"

# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
s5cmd cp -sp "s3://allennlp-mattj/scratch/test_all_dressed/$X/*" "/mnt/raid0/$X"
mkdir -p "/mnt/raid0/${X}_output"

# Step 2: Run the map operation
echo "Running map operation..."
cd ~/datamap-rs
git checkout dclm
cargo run --release -- map --input-dir "/mnt/raid0/$X" --output-dir "/mnt/raid0/${X}_output" --config examples/all_dressed/config.yaml > "/mnt/raid0/${X}_output/map.log"

# Step 3: Run the deduplication operation on JUST the outputs 
echo "Running deduplication..."
cd ~/minhash-rs
git checkout refac2025 
cargo run --release -- exact-dedup  --config examples/all_dressed/ed_stub.yaml --input-dir-override "/mnt/raid0/${X}_output/step_final" --output-dir-override "/mnt/raid0/${X}_output/step_final_exactdedup" > "/mnt/raid0/${X}_output/exactdedup.log"


# Step 4: Reshard the output data to be a better size 
cd ~/datamap-rs
git checkout reshardh
cargo run --release -- reshard --input-dir "/mnt/raid0/${X}_output/step_final_exactdedup/" --output-dir "/mnt/raid0/${X}_output/step_final_exactdedup_reshard/" --max-lines 65536

# Step 5: Copy results back to S3
echo "Copying results back to S3..."
s5cmd cp -sp "/mnt/raid0/${X}_output/step_final_exactdedup_reshard/" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed/$X/"
s5cmd cp -sp "/mnt/raid0/${X}_output/step_12/" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/non_english/$X/"

# Step 6: Clean up local storage
echo "Cleaning up local storage..."
rm -rf "/mnt/raid0/${X}_output"
rm -rf "/mnt/raid0/$X"
rm -rf "/mnt/raid0/ed_working_dir"

echo "Processing complete for $X"