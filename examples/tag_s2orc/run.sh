#!/bin/bash

set -e

echo "Running tag operation..."
cargo run --release -- map --input-dir /mnt/raid0/s2orc_full_0625 --output-dir /mnt/raid0/s2orc_full_0625_fos_tagged  --config examples/tag_s2orc/tag-docs.yaml 