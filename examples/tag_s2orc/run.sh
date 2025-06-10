#!/bin/bash

set -e

#echo "Running tag operation..."
#cargo run --release -- map --input-dir /mnt/raid0/s2orc_full_0625 --output-dir /mnt/raid0/s2orc_full_0625_fos_tagged  --config examples/tag_s2orc/tag-docs.yaml 

#echo "Running partition"
#cargo run --release -- partition --input-dir /mnt/raid0/s2orc_full_0625_fos_tagged/step_final/ --output-dir /mnt/raid0/s2orc_full_0625_fos_tagged_partitioned --config examples/tag_s2orc/partition-docs.yaml

OUTPUT_DIR="/mnt/raid0/s2orc_full_0625_fos_tagged_partitioned_dirs"

echo "Looking for files matching pattern: /mnt/raid0/s2orc_full_0625_fos_tagged_partitioned/chunk___*__*.jsonl.zst"
for file in /mnt/raid0/s2orc_full_0625_fos_tagged_partitioned/chunk___*__*.jsonl.zst; do
  # Extract the label from the filename
  label=$(basename "$file" | sed 's/.*__\([^.]*\)\..*/\1/')

  # Extract the new filename (remove chunk___*__ prefix)
  new_filename=$(basename "$file" | sed 's/chunk___[^_]*__//')

  # Create directory if it doesn't exist
  mkdir -p "$OUTPUT_DIR/$label"

  # Move the file
  mv "$file" "$OUTPUT_DIR/$label/$new_filename"

  echo "Moved $file to $OUTPUT_DIR/$label/$new_filename"
done