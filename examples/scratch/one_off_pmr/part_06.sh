#!/bin/bash



# Store the input argument
X="06"


echo "Processing directory: $X"




# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/groups"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/scratch/groupsort_downloader/downloader_part_00${X}.txt" "/mnt/raid0/part_${X}.txt"
s5cmd run "/mnt/raid0/part_${X}.txt"

# Step 2: Run the map operation
echo "Running group operation..."

# git clone https://github.com/revbucket/minhash-rs.git
cd ~/datamap-rs
git checkout sort; git pull;
#git checkout lowermem_cc

cargo run --release -- dist-group --input-dir /mnt/raid0/input --group-dir /mnt/raid0/groups --config examples/all_dressed/ad_groupsort_config.yaml  --subext "part_${X}" > "/mnt/raid0/part_${X}_output.log"


# Step 5: Copy results back to S3
# S3 file structure looks like ... :
# s3://ai2-llm/pretraining-data/sources/cc_all_dressed/
#     - all_dressed_v2/english/{CC_DUMP}/*.jsonl.*
#     - all_dressed_v2/logs/{CC_DUMP}/*.txt

echo "Copying results back to S3..."



s5cmd cp -sp /mnt/raid0/groups/ s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/scratch/group_00/

echo "Processing complete for $X"