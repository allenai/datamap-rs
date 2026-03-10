#!/usr/bin/env bash
set -euo pipefail

INPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned"
OUTPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned_decon"

DRY_RUN=false
PARALLEL=4

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        --parallel) PARALLEL="$2"; shift 2 ;;
        *)          echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if $DRY_RUN; then
    echo "=== DRY RUN MODE ==="
fi
echo "=== Parallelism: $PARALLEL ==="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG="$SCRIPT_DIR/decon_filter.yaml"

# Build once upfront so parallel jobs don't race on compilation
if ! $DRY_RUN; then
    echo "Building datamap-rs..."
    cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml"
fi

PIDS=()
FAILURES=0

# Wait until fewer than $PARALLEL jobs are running
wait_for_slot() {
    while (( ${#PIDS[@]} >= PARALLEL )); do
        local new_pids=()
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                new_pids+=("$pid")
            else
                wait "$pid" || (( FAILURES++ ))
            fi
        done
        PIDS=("${new_pids[@]}")
        if (( ${#PIDS[@]} >= PARALLEL )); then
            sleep 1
        fi
    done
}

# Wait for all remaining jobs
wait_all() {
    for pid in "${PIDS[@]}"; do
        wait "$pid" || (( FAILURES++ ))
    done
    PIDS=()
}

# Find all leaf directories containing shard files
find "$INPUT_DIR" -name '*.jsonl.zst' -printf '%h\n' | sort -u | while read -r input_path; do
    rel_path="${input_path#"$INPUT_DIR"/}"
    # Use a temp output dir so we can flatten step_final/ away
    tmp_output="$OUTPUT_DIR/.tmp_$RANDOM/$rel_path"
    final_output="$OUTPUT_DIR/$rel_path"

    cmd=(
        cargo run --release --manifest-path "$REPO_ROOT/Cargo.toml" --
        map
        --input-dir "$input_path"
        --output-dir "$tmp_output"
        --config "$CONFIG"
    )

    if $DRY_RUN; then
        echo "${cmd[*]}"
    else
        echo "Running: $rel_path"
        wait_for_slot
        (
            "${cmd[@]}"
            # Move step_final contents to the real output, removing the step_final layer
            mkdir -p "$final_output"
            mv "$tmp_output/step_final"/* "$final_output"/
            rm -rf "$tmp_output"
        ) &
        PIDS+=($!)
    fi
done

wait_all

# Clean up any leftover tmp dirs
find "$OUTPUT_DIR" -maxdepth 1 -name '.tmp_*' -type d -exec rm -rf {} + 2>/dev/null || true

if (( FAILURES > 0 )); then
    echo ""
    echo "=== Done with $FAILURES failure(s) ==="
    exit 1
fi

echo ""
echo "=== Done ==="
