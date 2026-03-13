#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_dir> <output_dir>"
    exit 1
fi

input_dir="$1"
output_dir="$2"

script_dir="$(cd "$(dirname "$0")" && pwd)"
config_dir="${script_dir}/vigintiles"

for config in "${config_dir}"/*.yaml; do
    topic="$(basename "${config}" .yaml)"
    echo "Processing ${topic}..."
    cargo run --release -- map \
        --input-dir "${input_dir}/${topic}" \
        --output-dir "${output_dir}/${topic}" \
        --config "configs/rest_web/vigintiles/${topic}.yaml"
done
