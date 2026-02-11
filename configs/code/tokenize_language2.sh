#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"
INPUT_DIR="pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_filter_v2_2026_stack_edu_redux_tagged_partitioned"

FIELD_ID="blob_id"
EXTENSION="*"
TOKENIZER_NAME="allenai/dolma2-tokenizer"

# ============================================================================
# Get instance rank and world size from EC2 metadata
# ============================================================================

# Get instance ID from EC2 metadata
INSTANCE_ID=$(ec2-metadata --instance-id | grep -oP 'instance-id: \K(i-[a-f0-9]+)')
echo "Instance ID: ${INSTANCE_ID}"

# Get instance name using AWS CLI
INSTANCE_NAME=$(aws ec2 describe-tags \
    --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=Name" \
    --query "Tags[0].Value" --output text)
echo "Instance Name: ${INSTANCE_NAME}"

# Extract group name (everything except the last 4 digits) and instance index (last 4 digits)
# Instance name format: <group name>-<4 digit index>, e.g., "my-group-0001"
GROUP_NAME=$(echo "${INSTANCE_NAME}" | sed 's/-[0-9]\{4\}$//')
INSTANCE_IDX=$(echo "${INSTANCE_NAME}" | grep -oP '[0-9]{4}$')
# Remove leading zeros to get numeric rank
RANK=$((10#${INSTANCE_IDX}))
echo "Group Name: ${GROUP_NAME}"
echo "Instance Index: ${INSTANCE_IDX} (Rank: ${RANK})"

# Get world size by counting all instances with the same group name prefix
WORLD_SIZE=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${GROUP_NAME}-*" "Name=instance-state-name,Values=running" \
    --query "Reservations[*].Instances[*].InstanceId" --output text | wc -w)
echo "World Size: ${WORLD_SIZE}"

# If there's only one instance, force rank to 0
if [ "${WORLD_SIZE}" -eq 1 ]; then
    RANK=0
    echo "Single instance detected, setting RANK to 0"
fi


# ============================================================================
# Define all languages and compute subset for this instance
# ============================================================================

ALL_LANGUAGES=(
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
# Compute which languages this instance should process
# Each instance processes languages where (language_index % world_size) == rank
LANGUAGES=()
for i in "${!ALL_LANGUAGES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq ${RANK} ]; then
        LANGUAGES+=("${ALL_LANGUAGES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${LANGUAGES[*]}"

# figure out where to output files
OUTPUT_DIR=$(echo "${INPUT_DIR}" | sed 's|^pretraining-data/sources|preprocessed|')
echo "Output directory: ${OUTPUT_DIR}"

# ============================================================================
# Setup environment
# ============================================================================

if [ ! -d ".venv" ]; then
    # setting up virtual environment
    uv venv
fi

# installing dolma
uv pip install dolma backports-zstd backports-weakref

# downloading dolma2-tokenizer
uv run --with=huggingface-hub \
    hf download ${TOKENIZER_NAME} \
    --local-dir ${LOCAL_DIR}/huggingface/${TOKENIZER_NAME}

# ============================================================================
# Process languages: tokenization
# ============================================================================

for language in "${LANGUAGES[@]}"; do
    local_input_dir="${LOCAL_DIR}/${INPUT_DIR}/${language}"
    local_output_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"

    # download files if not present locally
    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${INPUT_DIR}/${language}"
        echo "Downloading ${language} from ${remote_input_dir}..."
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    if [ ! -d "${local_input_dir}" ]; then
        echo "Input directory ${local_input_dir} not found... Skipping ${language}"
        continue
    fi

    for step_dir in $(ls --color=never "${local_input_dir}"); do
        num_processes=$(python3 -c "import multiprocessing; print(multiprocessing.cpu_count())")
        # Cap num_processes by directory size / 100MB
        dir_size_bytes=$(du -sb "${local_input_dir}/${step_dir}" | awk '{print $1}')
        size_based_procs=$(python3 -c "import math; print(max(1, math.floor(${dir_size_bytes} / (100 * 1024 * 1024))))")
        num_processes=$(( num_processes < size_based_procs ? num_processes : size_based_procs ))

        # tokenizing the language
        uv run dolma tokens \
            --documents "${local_input_dir}/${step_dir}/${EXTENSION}" \
            --destination "${local_output_dir}/${step_dir}/${TOKENIZER_NAME}" \
            --tokenizer.name_or_path ${TOKENIZER_NAME} \
            --tokenizer.eos_token_id 100257 \
            --tokenizer.pad_token_id 100277 \
            --fields.id_field_name ${FIELD_ID} \
            --no-tokenizer.segment_before_tokenization \
            --tokenizer.encode_special_tokens \
            --processes ${num_processes} \
            --max_size 4_000_000_000 \
            --sample_ring_prop \
            --dtype uint32
    done
done

# ============================================================================
# Upload results to S3
# ============================================================================

echo "Uploading results to S3..."
for language in "${LANGUAGES[@]}"; do
    local_dir="${LOCAL_DIR}/${OUTPUT_DIR}/${language}"
    s3_dir="${REMOTE_DIR}/${OUTPUT_DIR}/${language}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${language} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done

echo "Done!"
