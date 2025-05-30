#!/bin/bash


# Store the input argument
X=22


echo "Processing directory: $X"






# Step 0: preclean
echo "Pre-clean local storage..."
rm -rf "/mnt/raid0/input"
rm -rf "/mnt/raid0/output"




# Step 1: Copy from S3 to local storage
echo "Copying data from S3 to local storage..."
cd ~/datamap-rs
git checkout main; git pull
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/exact_dedup/${X}/*" /mnt/raid0/input/

cargo run --release -- reshard --input-dir /mnt/raid0/input --output-dir /mnt/raid0/output --max-size 256000000 --subsample 0.03

echo "Copying results back to S3..."
s5cmd cp -sp /mnt/raid0/output/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/exact_deduped_subsample0.03/${X}/"


echo "Processing complete for $X"