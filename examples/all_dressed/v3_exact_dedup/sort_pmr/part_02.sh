#!/bin/bash


# Store the input argument
X=02


echo "Processing directory: $X"

# Step -1:
TARGET_DIR="s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/exact_dedup_v2/$X/"
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
rm -rf "/mnt/raid0/sorted"
rm -rf "/mnt/raid0/output"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
cd ~/datamap-rs
git checkout sort_v2; git pull
python3 examples/all_dressed/v3_exact_dedup/sort_download_builder.py $X
s5cmd run "/mnt/raid0/downloader_${X}.txt"



# Step 2: Run the map operation
echo "Running sort/filter operations..."

cargo run --release -- dist-sort --input-dir /mnt/raid0/input --output-dir /mnt/raid0/sorted  --config examples/all_dressed/v3_exact_dedup/groupsort_config.yaml  > "/mnt/raid0/sort_${X}.log"
cargo run --release -- group-sort-filter --input-dir /mnt/raid0/sorted --output-dir /mnt/raid0/output  --config examples/all_dressed/v3_exact_dedup/groupsort_config.yaml  > "/mnt/raid0/filtered_${X}.log"



echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/sorted/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/ed_sorted_v2/${X}/"
s5cmd cp -sp /mnt/raid0/output/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/exact_dedup_v2/${X}/"
s5cmd cp -sp "/mnt/raid0/*.log" s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/logs/

echo "Processing complete for $X"