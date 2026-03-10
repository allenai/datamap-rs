#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned_decon/finepdfs_wo/step_final"
OUTPUT_DIR="${BASE_DIR}_combined"

DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        *)          echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if $DRY_RUN; then
    echo "=== DRY RUN MODE ==="
fi

mkdir -p "$OUTPUT_DIR"

COPIED=0

for i in $(seq -w 0 19); do
    qual="qual_00${i}"
    src_dir="${BASE_DIR}/${qual}"

    if [[ ! -d "$src_dir" ]]; then
        echo "WARNING: $src_dir does not exist, skipping."
        continue
    fi

    for filepath in "$src_dir"/*; do
        [[ -f "$filepath" ]] || continue
        filename="$(basename "$filepath")"
        dest="${OUTPUT_DIR}/${qual}_${filename}"

        if $DRY_RUN; then
            echo "cp $filepath -> $dest"
        else
            cp "$filepath" "$dest"
            (( COPIED++ )) || true
        fi
    done
done

echo "=== Done — copied $COPIED files into $OUTPUT_DIR ==="
