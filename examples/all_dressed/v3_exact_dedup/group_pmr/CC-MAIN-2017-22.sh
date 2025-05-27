#!/bin/bash


# Store the input argument
X=CC-MAIN-2017-22


echo "Processing directory: $X"

# Step -1:
TARGET_DIR="s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/grouped/$X/"
FILE_COUNT=$(aws s3 ls "$TARGET_DIR" | wc -l)

if [ "$FILE_COUNT" -gt 0 ]; then
    echo "Files exist in the prefix: $TARGET_DIR (found $FILE_COUNT items)"
    exit 0
else
    echo "No files found in the prefix: $TARGET_DIR -- proceeding with processing"
fi






# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/groups"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."


s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/english/${X}/*" "/mnt/raid0/input/"

# Step 2: Run the map operation
echo "Running group operation..."
cd
# git clone https://github.com/revbucket/minhash-rs.git
cd ~/datamap-rs
git checkout sort; git pull;
#git checkout lowermem_cc

cargo run --release -- dist-group --input-dir /mnt/raid0/input --group-dir /mnt/raid0/groups --config examples/all_dressed/v3_exact_dedup/groupsort_config.yaml  --subext "${X}" > "/mnt/raid0/group_${X}.log"


echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/groups/ "${TARGET_DIR}"
s5cmd cp -sp "/mnt/raid0/*.log" s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/logs/

echo "Processing complete for $X"