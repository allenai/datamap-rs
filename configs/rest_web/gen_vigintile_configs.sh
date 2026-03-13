#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <bounds_dir>"
    echo "Example: $0 ~/all_web_vigintile_bounds"
    exit 1
fi

bounds_dir="$1"
script_dir="$(cd "$(dirname "$0")" && pwd)"
out_dir="${script_dir}/vigintiles"
mkdir -p "${out_dir}"

for json_file in "${bounds_dir}"/*.json; do
    topic="$(basename "${json_file}" .json)"
    out_file="${out_dir}/${topic}.yaml"

    # parse JSON array into bash array
    mapfile -t thresholds < <(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    for v in json.load(f):
        print(v)
" "${json_file}")

    # write yaml
    {
        echo "name: quality_vigintiles"
        echo "text_field: text"
        echo "pipeline:"
        for i in "${!thresholds[@]}"; do
            printf '    - name: float_filter\n'
            printf '      step: vigintile_%04d\n' "$i"
            printf '      kwargs:\n'
            printf '          float_field: metadata.ultrafineweb.__label__pos\n'
            printf '          lower_bound: %s\n' "${thresholds[$i]}"
        done
        # final vigintile with sentinel bound
        printf '    - name: float_filter\n'
        printf '      step: vigintile_%04d\n' "${#thresholds[@]}"
        printf '      kwargs:\n'
        printf '          float_field: metadata.ultrafineweb.__label__pos\n'
        printf '          lower_bound: 1000\n'
    } > "${out_file}"

    echo "Generated ${out_file}"
done
