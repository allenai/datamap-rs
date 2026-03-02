#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
OUTPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

QUALITY_EXPRESSION=".metadata.stack_edu_redux_combined"
GZIP_EXPRESSION=".metadata.gzip_compression_ratio"
WEIGHT_BY='.text | length'
NUM_SAMPLES=10000000

# By default, recompute reports even if they already exist.
FORCE_RERUN="${FORCE_RERUN:-1}"

# ============================================================================
# Get instance rank and world size from EC2 metadata
# ============================================================================

INSTANCE_ID=$(ec2-metadata --instance-id | grep -oP 'instance-id: \K(i-[a-f0-9]+)')
echo "Instance ID: ${INSTANCE_ID}"

INSTANCE_NAME=$(aws ec2 describe-tags \
    --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=Name" \
    --query "Tags[0].Value" --output text)
echo "Instance Name: ${INSTANCE_NAME}"

GROUP_NAME=$(echo "${INSTANCE_NAME}" | sed 's/-[0-9]\{4\}$//')
INSTANCE_IDX=$(echo "${INSTANCE_NAME}" | grep -oP '[0-9]{4}$')
RANK=$((10#${INSTANCE_IDX}))
echo "Group Name: ${GROUP_NAME}"
echo "Instance Index: ${INSTANCE_IDX} (Raw Rank: ${RANK})"

WORLD_SIZE=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${GROUP_NAME}-*" "Name=instance-state-name,Values=running" \
    --query "Reservations[*].Instances[*].InstanceId" --output text | wc -w)
echo "World Size: ${WORLD_SIZE}"

if [ "${WORLD_SIZE}" -eq 0 ]; then
    echo "No running instances found for group ${GROUP_NAME}"
    exit 1
fi

if [ "${WORLD_SIZE}" -eq 1 ]; then
    RANK=0
    echo "Single instance detected, setting RANK to 0"
else
    RANK=$((RANK % WORLD_SIZE))
    echo "Normalized Rank: ${RANK}"
fi

# ============================================================================
# Define all languages from tokenize_language.sh + tokenize_language2.sh
# and compute this-instance subset
# ============================================================================

ALL_LANGUAGES=(
    "C"
    "C++"
    "C-Sharp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Python"
    "Ruby"
    "Rust"
    "Shell"
    "SQL"
    "Swift"
    "TypeScript"
    "Blade"
    "Bluespec"
    "CSS"
    "Clojure"
    "Common_Lisp"
    "Cuda"
    "Dart"
    "Erlang"
    "Fortran"
    "Fortran_Free_Form"
    "Haskell"
    "Java_Server_Pages"
    "Julia"
    "Kotlin"
    "Lua"
    "MATLAB"
    "Mathematica"
    "OCaml"
    "Objective-C"
    "OpenCL"
    "Pascal"
    "Perl"
    "R"
    "RMarkdown"
    "SCSS"
    "Scala"
    "Scheme"
    "SystemVerilog"
    "Tcl"
    "VHDL"
    "Verilog"
    "Vue"
    "html"
    "jupyter_notebook"
    "reStructuredText"
)

echo "Loaded ${#ALL_LANGUAGES[@]} languages"

LANGUAGES=()
for i in "${!ALL_LANGUAGES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq "${RANK}" ]; then
        LANGUAGES+=("${ALL_LANGUAGES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${LANGUAGES[*]}"

# ============================================================================
# Process languages: percentile reports
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    local_language_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"
    local_step_final_dir="${local_language_dir}/step_final"
    remote_language_dir="${REMOTE_DIR}/${OUTPUT_DIR}/${language}"
    quality_report_file="${local_language_dir}/code_quality_report.yaml"
    gzip_report_file="${local_language_dir}/gzip_compression_report.yaml"

    if [ ! -d "${local_step_final_dir}" ]; then
        echo "Downloading ${language} from ${remote_language_dir}/step_final/..."
        mkdir -p "${local_step_final_dir}"
        if ! s5cmd cp -sp "${remote_language_dir}/step_final/*" "${local_step_final_dir}/"; then
            echo "Failed to download step_final for ${language}... Skipping"
            continue
        fi
    fi

    if [ ! -d "${local_step_final_dir}" ]; then
        echo "Input directory ${local_step_final_dir} not found... Skipping ${language}"
        continue
    fi

    mkdir -p "${local_language_dir}"

    if [ "${FORCE_RERUN}" = "1" ] || [ ! -f "${quality_report_file}" ]; then
        echo "Determining vigintiles for ${language} quality score..."
        uv run "${REPO_ROOT}/python/percentile.py" \
            "${local_step_final_dir}" \
            --output-file "${quality_report_file}" \
            --expression "${QUALITY_EXPRESSION}" \
            --weight-by "${WEIGHT_BY}" \
            --num-samples "${NUM_SAMPLES}"
    else
        echo "Output file ${quality_report_file} already exists, skipping"
    fi

    if [ -f "${quality_report_file}" ]; then
        echo "Uploading ${quality_report_file} to ${remote_language_dir}/code_quality_report.yaml..."
        s5cmd cp -sp "${quality_report_file}" "${remote_language_dir}/code_quality_report.yaml"
    fi

    if [ "${FORCE_RERUN}" = "1" ] || [ ! -f "${gzip_report_file}" ]; then
        echo "Determining vigintiles for ${language} gzip compression ratio..."
        uv run "${REPO_ROOT}/python/percentile.py" \
            "${local_step_final_dir}" \
            --output-file "${gzip_report_file}" \
            --expression "${GZIP_EXPRESSION}" \
            --weight-by "${WEIGHT_BY}" \
            --num-samples "${NUM_SAMPLES}"
    else
        echo "Output file ${gzip_report_file} already exists, skipping"
    fi

    if [ -f "${gzip_report_file}" ]; then
        echo "Uploading ${gzip_report_file} to ${remote_language_dir}/gzip_compression_report.yaml..."
        s5cmd cp -sp "${gzip_report_file}" "${remote_language_dir}/gzip_compression_report.yaml"
    fi
done

echo "Done!"
