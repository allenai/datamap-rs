#!/bin/bash



# Store the input argument
X=00


echo "Processing prt: $X"

# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/ts2"
rm -rf "/mnt/raid0/ts2_output"

sudo yum install go -y


# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
s5cmd cp "s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/partition_downloaders/ts2_download_part${X}.txt" .
s5cmd run "ts2_download_part${X}.txt"


# Step 2: Run the map operation
echo "Running map operation..."
cd ~/datamap-rs
git checkout spring2code_remote
git config --global url."https://github.com/".insteadOf "git@github.com:"
git clone --recurse-submodules https://github.com/go-enry/rs-enry
git submodule init
git submodule update

cargo run --release -- map --input-dir "/mnt/raid0/ts2" --output-dir "/mnt/raid0/ts2_output" --config examples/spring2code/config.yaml > "/mnt/raid0/part_${X}.log"

echo "Copying results back to S3..."
s5cmd cp -sp "/mnt/raid0/ts2_output/step_final/" s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/data/
#s5cmd cp -sp "/mnt/raid0/${X}_output/step_12/" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed/non_english/$X/"
s5cmd cp -sp "/mnt/raid0/part_${X}.log" s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/logs/

# Step 6: Clean up local storage
echo "Cleaning up local storage..."
rm -rf "/mnt/raid0/ts2"
rm -rf "/mnt/raid0/ts2_output"

echo "Processing complete for $X"