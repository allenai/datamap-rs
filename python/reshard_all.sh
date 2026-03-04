#!/usr/bin/env bash
set -euo pipefail

INPUT_BASE="/mnt/raid0/url_and_mh_grouped_priority_filtered_partitioned"
OUTPUT_BASE="/mnt/raid0/url_and_mh_grouped_priority_filtered_partitioned_resharded"
DATAMAP_DIR="/home/ec2-user/datamap-rs"
MAX_SIZE=256000000

# Find all leaf directories (ones that contain .jsonl.zst files directly)
LEAF_DIRS=()
while IFS= read -r dir; do
    LEAF_DIRS+=("$dir")
done < <(find "$INPUT_BASE" -name '*.jsonl.zst' -printf '%h\n' | sort -u)

echo "Found ${#LEAF_DIRS[@]} directories to reshard"
echo "Output base: $OUTPUT_BASE"
echo ""

FAILED=0
SUCCEEDED=0

for dir in "${LEAF_DIRS[@]}"; do
    rel="${dir#$INPUT_BASE/}"
    out_dir="$OUTPUT_BASE/$rel"

    n_files=$(ls "$dir"/*.jsonl.zst 2>/dev/null | wc -l)
    echo "=== [$((SUCCEEDED + FAILED + 1))/${#LEAF_DIRS[@]}] $rel ($n_files files) ==="

    mkdir -p "$out_dir"

    if cargo run --release -- reshard \
        --input-dir "$dir" \
        --output-dir "$out_dir" \
        --max-size "$MAX_SIZE"; then
        SUCCEEDED=$((SUCCEEDED + 1))
        echo "  -> OK"
    else
        FAILED=$((FAILED + 1))
        echo "  -> FAILED"
    fi
    echo ""
done

echo "Done. $SUCCEEDED succeeded, $FAILED failed out of ${#LEAF_DIRS[@]} directories."
