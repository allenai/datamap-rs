#!/bin/bash



# Store the input argument
X="01"
Y="a"

echo "Processing directory: $X"




# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/groups"
rm -rf "/mnt/raid0/sorted"
rm -rf "/mnt/raid0/filtered"



# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."

# git clone https://github.com/revbucket/minhash-rs.git
cd ~/datamap-rs
git checkout sort_v2; git pull;
python3 examples/all_dressed/sub025_grouper/make_download_part.py $X $Y
s5cmd run download_script.txt


#git checkout lowermem_cc

cargo run --release -- dist-sort --input-dir /mnt/raid0/groups --output-dir /mnt/raid0/sorted --config examples/all_dressed/minhash_groupsort.yaml > "/mnt/raid0/part_${X}_${Y}_sort_output.log"
cargo run --release -- group-sort-filter --input-dir /mnt/raid0/sorted --output-dir /mnt/raid0/filtered --config examples/all_dressed/minhash_groupsort.yaml > "/mnt/raid0/part_${X}_${Y}_filter_output.log"


# Step 5: Copy results back to S3
# S3 file structure looks like ... :
# s3://ai2-llm/pretraining-data/sources/cc_all_dressed/
#     - all_dressed_v2/english/{CC_DUMP}/*.jsonl.*
#     - all_dressed_v2/logs/{CC_DUMP}/*.txt

echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/filtered/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/ed_sub0.25_minhash2x_2611/data/part_${Y}/"
s5cmd cp -sp "/mnt/raid0/*.log" s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/ed_sub0.25_minhash2x_2611/logs/

echo "Processing complete for $X"