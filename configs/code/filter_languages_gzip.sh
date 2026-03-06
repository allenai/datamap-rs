#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR=${REMOTE_DIR:-"s3://ai2-llm"}
LOCAL_DIR=${LOCAL_DIR:-"/mnt/raid0/ai2-llm"}
INPUT_DIR="${INPUT_DIR:-"pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged"}
OUTPUT_DIR=${OUTPUT_DIR:-"${INPUT_DIR}_partitioned_gzip"}
CONFIGS_DIR=${CONFIGS_DIR:-"configs/code/filters_gzip"}

USE_S3=1
if [ -z "${REMOTE_DIR}" ]; then
    USE_S3=0
    echo "REMOTE_DIR is empty; S3 download/upload disabled"
fi

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
# Define all languages and compute subset for this instance
# ============================================================================

ALL_LANGUAGES=(
    "Python"
    "Blade"
    "Bluespec"
    "C"
    "C-Sharp"
    "C++"
    "Clojure"
    "Common_Lisp"
    "CSS"
    "Cuda"
    "Dart"
    "Erlang"
    "Fortran"
    "Fortran_Free_Form"
    "Go"
    "Haskell"
    "html"
    "Java"
    "Java_Server_Pages"
    "JavaScript"
    "Julia"
    "jupyter_notebook"
    "Kotlin"
    "Lua"
    "Markdown"
    "Mathematica"
    "MATLAB"
    "Objective-C"
    "OCaml"
    "OpenCL"
    "Pascal"
    "Perl"
    "PHP"
    "R"
    "reStructuredText"
    "RMarkdown"
    "Ruby"
    "Rust"
    "Scala"
    "Scheme"
    "SCSS"
    "Shell"
    "SQL"
    "Swift"
    "SystemVerilog"
    "Tcl"
    "TypeScript"
    "Verilog"
    "VHDL"
    "Vue"
)

LANGUAGES=()
for i in "${!ALL_LANGUAGES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq "${RANK}" ]; then
        LANGUAGES+=("${ALL_LANGUAGES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${LANGUAGES[*]}"

config_name_for_language() {
    local language="$1"
    case "${language}" in
        "C++")
            echo "cpp.yaml"
            ;;
        "C-Sharp")
            echo "csharp.yaml"
            ;;
        "Objective-C")
            echo "objective_c.yaml"
            ;;
        *)
            echo "${language}" | tr '[:upper:]' '[:lower:]' | tr '-' '_' | sed 's/$/.yaml/'
            ;;
    esac
}

# ============================================================================
# Process languages: filtering
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    config_name="$(config_name_for_language "${language}")"
    config_file="${CONFIGS_DIR}/${config_name}"

    if [ ! -f "${config_file}" ]; then
        echo "Config file ${config_file} not found... Skipping ${language}"
        continue
    fi

    local_input_dir="${LOCAL_DIR}/${INPUT_DIR}/${language}"
    local_output_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"

    if [ -d "${local_output_dir}" ]; then
        echo "Output directory ${local_output_dir} already exists"
        continue
    fi

    if [ ! -d "${local_input_dir}" ]; then
        if [ "${USE_S3}" -eq 0 ]; then
            echo "Input directory ${local_input_dir} not found and REMOTE_DIR is empty... Skipping ${language}"
            continue
        fi

        remote_input_dir="${REMOTE_DIR}/${INPUT_DIR}/${language}"
        mkdir -p "${local_input_dir}"
        if ! s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"; then
            echo "Failed to download ${language} from ${remote_input_dir}... Skipping"
            continue
        fi
    fi

    # Until there is no nested "step_final" dir, keep descending.
    while true; do
        if [ -d "${local_input_dir}/step_final" ]; then
            local_input_dir="${local_input_dir}/step_final"
        else
            break
        fi
    done

    echo "Filtering ${language} with config ${config_file}..."
    cargo run --release map \
        --input-dir "${local_input_dir}" \
        --output-dir "${local_output_dir}" \
        --config "${config_file}"

    if [ "${USE_S3}" -eq 1 ]; then
        s3_output_dir="${REMOTE_DIR}/${OUTPUT_DIR}/${language}"
        echo "Uploading ${language} to ${s3_output_dir}..."
        s5cmd cp -sp "${local_output_dir}/*" "${s3_output_dir}/"
    else
        echo "Skipping upload for ${language} because REMOTE_DIR is empty"
    fi
done

echo "Done!"
