#!/bin/bash



# Store the input argument
X=crawl=CC-MAIN-2022-21
echo "Processing directory: $X"

# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/${X}_output"
rm -rf "/mnt/raid0/$X"
rm -rf "/mnt/raid0/ed_working_dir"


# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
s5cmd cp -sp "s3://ai2-oe-data/contrib/datacomp/DCLM-pool/$X/*" "/mnt/raid0/$X"
mkdir -p "/mnt/raid0/${X}_output"

# Step 2: Run the map operation
echo "Running map operation..."
cd ~/datamap-rs
git checkout main
cargo run --release -- map --input-dir "/mnt/raid0/$X" --output-dir "/mnt/raid0/${X}_output" --config examples/all_dressed/config.yaml > "/mnt/raid0/${X}_output/map.log"

# Step 3: Run the deduplication operation on JUST the outputs 
echo "Running deduplication..."
cd ~/minhash-rs
git checkout refac2025 
cargo run --release -- exact-dedup  --config examples/all_dressed/ed_stub.yaml --input-dir-override "/mnt/raid0/${X}_output/step_final" --output-dir-override "/mnt/raid0/${X}_output/step_final_exactdedup" > "/mnt/raid0/${X}_output/exactdedup.log"


# Step 4: Reshard the output data to be a better size 
cd ~/datamap-rs
git checkout main
cargo run --release -- reshard --input-dir "/mnt/raid0/${X}_output/step_final_exactdedup/" --output-dir "/mnt/raid0/${X}_output/step_final_exactdedup_reshard/" --max-lines 65536

# Step 5: Copy results back to S3
# S3 file structure looks like ... :
# s3://ai2-llm/pretraining-data/sources/cc_all_dressed/
#     - all_dressed_v2/english/{CC_DUMP}/*.jsonl.*
#     - all_dressed_v2/logs/{CC_DUMP}/*.txt

echo "Copying results back to S3..."
s5cmd cp -sp "/mnt/raid0/${X}_output/step_final_exactdedup_reshard/" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/english/$X/"
#s5cmd cp -sp "/mnt/raid0/${X}_output/step_12/" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed/non_english/$X/"
s5cmd cp -sp "/mnt/raid0/${X}_output/*.log" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/logs/$X/"

# Step 6: Clean up local storage
echo "Cleaning up local storage..."
rm -rf "/mnt/raid0/${X}_output"
rm -rf "/mnt/raid0/$X"
rm -rf "/mnt/raid0/ed_working_dir"

echo "Processing complete for $X"