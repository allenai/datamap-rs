#!/bin/bash



# Store the input argument
X="crawl=CC-MAIN-2019-09"


echo "Processing directory: $X"

# Step -1:
TARGET_DIR="s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/english_madlad25/$X/"
FILE_COUNT=$(aws s3 ls "$TARGET_DIR" | wc -l)

if [ "$FILE_COUNT" -gt 0 ]; then
    echo "Files exist in the prefix: $TARGET_DIR (found $FILE_COUNT items)"
    exit 0
else
    echo "No files found in the prefix: $TARGET_DIR -- proceeding with processing"
fi


# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/${X}_output"
rm -rf "/mnt/raid0/$X"


# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/english/$X/*" "/mnt/raid0/$X"
mkdir -p "/mnt/raid0/${X}_output"

# Step 2: Run the map operation
echo "Running map operation..."
cd ~/datamap-rs
git checkout main
git pull 
cargo run --release -- map --input-dir "/mnt/raid0/$X" --output-dir "/mnt/raid0/${X}_output" --config examples/all_dressed/madlad_config.yaml > "/mnt/raid0/${X}_output/madlad_map.log"


# Step 5: Copy results back to S3
# S3 file structure looks like ... :
# s3://ai2-llm/pretraining-data/sources/cc_all_dressed/
#     - all_dressed_v2/english/{CC_DUMP}/*.jsonl.*
#     - all_dressed_v2/logs/{CC_DUMP}/*.txt

echo "Copying results back to S3..."
s5cmd cp -sp "/mnt/raid0/${X}_output/step_final/" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/english_madlad25/$X/"
s5cmd cp -sp "/mnt/raid0/${X}_output/*.log" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/logs/$X/"

# Step 6: Clean up local storage
echo "Cleaning up local storage..."
rm -rf "/mnt/raid0/${X}_output"
rm -rf "/mnt/raid0/$X"

echo "Processing complete for $X"