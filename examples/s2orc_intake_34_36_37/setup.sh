#!/bin/bash

set -e

# First, the user must setup raid0 and cd /mnt/raid0
# Then s5cmd cp s3://ai2-llm/pretraining-data/sources/s2orc_full_0625_fos_tagged_partitioned/* s2orc_full_0625_fos_tagged_partitioned

# Grab the list of BLESSED paths that s2orc can have
aws s3 cp s3://ai2-oe-data/jakep/kylesqMay2025/all_take34_take36_take37_paths.txt /mnt/raid0/all_take34_take36_take37_paths.txt 


CATEGORIES=(
    agricultural-and-food-sciences
    computer-science
    geography
    materials-science
    political-science
    art
    economics
    geology
    mathematics
    psychology
    biology
    education
    history
    medicine
    sociology
    business
    engineering
    law
    philosophy
    chemistry
    environmental-science
    linguistics
    physics
)

for CATEGORY in "${CATEGORIES[@]}"; do
  echo "Processing $CATEGORY..."
  
 cargo run --release -- map \
    --input-dir "/mnt/raid0/s2orc_full_0625_fos_tagged_partitioned/${CATEGORY}" \
    --output-dir "/mnt/raid0/s2orc_full_0625_fos_tagged_partitioned_take34_36_37_filtered/${CATEGORY}/" \
    --config ./examples/s2orc_intake_34_36_37/config.yaml
done

