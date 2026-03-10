#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "=== DRY RUN MODE ==="
fi

# Find all step_final directories and move their contents up one level
find "$OUTPUT_DIR" -type d -name "step_final" | sort | while read -r step_dir; do
    parent="$(dirname "$step_dir")"
    echo "Moving contents of $step_dir -> $parent"

    if ! $DRY_RUN; then
        # Move all contents up, then remove the now-empty directory
        mv "$step_dir"/* "$parent"/
        rmdir "$step_dir"
    fi
done

echo ""
echo "=== Done ==="
