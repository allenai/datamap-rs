#!/bin/bash


# Store the input argument
X=Swift


echo "Processing directory: $X"







# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/groups"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."

s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2/data/${X}/*" "/mnt/raid0/input/"

# Step 2: Run the map operation
echo "Running group operation..."
cd
# git clone https://github.com/revbucket/minhash-rs.git
cd ~/datamap-rs
git checkout sort_v2; git pull;
#git checkout lowermem_cc

cargo run --release -- dist-group --input-dir /mnt/raid0/input --group-dir /mnt/raid0/groups --config examples/spring2code/minhash_groupsort.yaml  --subext "${X}" > "/mnt/raid0/group_${X}.log"
cargo run --release -- dist-sort --input-dir /mnt/raid0/groups --output-dir /mnt/raid0/sorted --config examples/spring2code/minhash_groupsort.yaml  > "/mnt/raid0/sort_${X}.log"
cargo run --release -- group-sort-filter --input-dir /mnt/raid0/sorted --output-dir /mnt/raid0/output --config examples/spring2code/minhash_groupsort.yaml  > "/mnt/raid0/filter_${X}.log"


echo "Copying results back to S3"
s5cmd cp -sp "/mnt/raid0/output/" "s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2/pruned/${X}/"
s5cmd cp -sp "/mnt/raid0/*.log" s3://ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2/logs/

echo "Processing complete for $X"