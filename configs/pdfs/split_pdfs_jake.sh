#!/usr/bin/env bash
set -euo pipefail

INPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged"
OUTPUT_DIR="/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned"

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

# Find all pdf_quality_report.yaml files recursively
find "$INPUT_DIR" -name "pdf_quality_report.yaml" -type f | sort | while read -r yaml_file; do
    echo ""
    echo "--- Processing: $yaml_file ---"

    dataset_dir="$(dirname "$yaml_file")"
    rel_path="${dataset_dir#"$INPUT_DIR"/}"

    input_path="$dataset_dir/step_final"
    output_path="$OUTPUT_DIR/$rel_path/step_final"

    # Extract vigintile boundaries (p5, p10, ..., p95) from value.percentiles
    range_groups=$(python3 -c "
import yaml, sys

with open('$yaml_file') as f:
    data = yaml.safe_load(f)

percentiles = data['value']['percentiles']

# Vigintiles: every 5th percentile from p5 to p95 (19 boundaries -> 20 groups)
keys = ['p5', 'p10', 'p15', 'p20', 'p25', 'p30', 'p35', 'p40', 'p45',
        'p50', 'p55', 'p60', 'p65', 'p70', 'p75', 'p80', 'p85', 'p90', 'p95']

values = []
for k in keys:
    if k not in percentiles:
        print(f'WARNING: missing {k} in {\"$yaml_file\"}', file=sys.stderr)
        continue
    values.append(str(percentiles[k]))

print(','.join(values))
")

    if [[ -z "$range_groups" ]]; then
        echo "WARNING: Could not extract percentiles from $yaml_file, skipping."
        continue
    fi

    cmd=(
        cargo run --release --manifest-path "$REPO_ROOT/Cargo.toml" --
        range-partition
        --input-dir "$input_path"
        --output-dir "$output_path"
        --value "metadata.combined_quality_score"
        --default-value 0.0
        --range-groups "$range_groups"
        --bucket-name "qual"
    )

    if $DRY_RUN; then
        echo "${cmd[*]}"
    else
        echo "Running: ${cmd[*]}"
        wait_for_slot
        "${cmd[@]}" &
        PIDS+=($!)
    fi
done

wait_all

if (( FAILURES > 0 )); then
    echo ""
    echo "=== Done with $FAILURES failure(s) ==="
    exit 1
fi

echo ""
echo "=== Done ==="
