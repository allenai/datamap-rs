#!/bin/bash



# Store the input argument
X="07"


echo "Processing directory: $X"




# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/groups"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/minhashed/part_${X}/*" "/mnt/raid0/input/"

# Step 2: Run the map operation
echo "Running map operation..."
cd
# git clone https://github.com/revbucket/minhash-rs.git
cd ~/datamap-rs
git checkout sort; git pull;
#git checkout lowermem_cc

cargo run --release -- dist-group --input-dir /mnt/raid0/input --group-dir /mnt/raid0/groups --config examples/all_dressed/ad_groupsort_config.yaml  --subext "part_${X}" > "/mnt/raid0/part_${X}_group_output.log"
cargo run --release -- dist-sort --input-dir /mnt/raid0/groups --output-dir /mnt/raid0/sorted --config examples/all_dressed/ad_groupsort_config.yaml > "/mnt/raid0/part_${X}_sort_output.log"
s5cmd cp -sp /mnt/raid0/sorted/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/sorted/part_${X}/"
rm -rf /mnt/raid0/input
cargo run --release -- group-sort-filter --input-dir /mnt/raid0/sorted --output-dir /mnt/raid0/filtered --config examples/all_dressed/ad_groupsort_config.yaml > "/mnt/raid0/part_${X}_filter_output.log"


# Step 5: Copy results back to S3
# S3 file structure looks like ... :
# s3://ai2-llm/pretraining-data/sources/cc_all_dressed/
#     - all_dressed_v2/english/{CC_DUMP}/*.jsonl.*
#     - all_dressed_v2/logs/{CC_DUMP}/*.txt

echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/filtered/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/filtered/part_${X}/"
s5cmd cp -sp "/mnt/raid0/*.log" s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/logs_v2/

echo "Processing complete for $X"