#!/usr/bin/env bash

set -euox pipefail

REMOTE_DIR="s3://ai2-llm"
LOCAL_DIR="/mnt/raid0/ai2-llm"

EXTENSION="*.zst"
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
# Define all sources and compute subset for this instance
# ============================================================================

ALL_SOURCES=(
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/bluespec"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/c"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/c#"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/c++"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/clojure"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/common-lisp"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/css"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/cuda"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/dart"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/erlang"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/fortran"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/go"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/haskell"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/html"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/java"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/java-server-pages"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/javascript"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/julia"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/jupyter-notebook"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/kotlin"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/lua"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/markdown"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/mathematica"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/matlab"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/objective-c++"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/ocaml"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/opencl"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/pascal"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/perl"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/php"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/python"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/r"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/restructuredtext"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/rmarkdown"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/ruby"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/rust"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/scala"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/scheme"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/scss"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/shell"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/sql"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/swift"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/systemverilog"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/tcl"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/typescript"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/vhdl"
    "bigcode_commitpack/dolma-3_5-languages_tagged_resharded_rewritten_partitioned/vue"
)

# Compute which sources this instance should process
# Each instance processes sources where (source_index % world_size) == rank
SOURCES=()
for i in "${!ALL_SOURCES[@]}"; do
    if [ $((i % WORLD_SIZE)) -eq ${RANK} ]; then
        SOURCES+=("${ALL_SOURCES[$i]}")
    fi
done

echo "This instance (rank ${RANK}/${WORLD_SIZE}) will process: ${SOURCES[*]}"

# ============================================================================
# Setup environment
# ============================================================================

if [ ! -d ".venv" ]; then
    # setting up virtual environment
    uv venv --python=3.12
fi


# installing dolma
uv pip install dolma backports-zstd backports-weakref

# downloading dolma2-tokenizer
uv run --with=huggingface-hub \
    hf download ${TOKENIZER_NAME} \
    --local-dir ${LOCAL_DIR}/huggingface/${TOKENIZER_NAME}

# ============================================================================
# Process sources: tokenization
# ============================================================================

for source in "${SOURCES[@]}"; do
    input_dir="pretraining-data/sources/${source}"
    output_dir=$(echo "${input_dir}" | sed 's|^pretraining-data/sources|preprocessed|')

    local_input_dir="${LOCAL_DIR}/${input_dir}"
    local_output_dir="${LOCAL_DIR}/${output_dir}"

    # download files if not present locally
    if [ ! -d "${local_input_dir}" ]; then
        remote_input_dir="${REMOTE_DIR}/${input_dir}"
        echo "Downloading ${source} from ${remote_input_dir}..."
        s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"
    fi

    if [ ! -d "${local_input_dir}" ]; then
        echo "Input directory ${local_input_dir} not found... Skipping ${source}"
        continue
    fi

    for step_dir in $(ls --color=never "${local_input_dir}"); do
        if [[ "${step_dir}" != quality_* ]]; then
            echo "Skipping ${step_dir} as it does not start with quality_"
            continue
        fi
        num_processes=$(python3 -c "import multiprocessing; print(multiprocessing.cpu_count())")
        # Cap num_processes by directory size / 100MB
        dir_size_bytes=$(du -sb "${local_input_dir}/${step_dir}" | awk '{print $1}')
        size_based_procs=$(python3 -c "import math; print(max(1, math.floor(${dir_size_bytes} / (100 * 1024 * 1024))))")
        num_processes=$(( num_processes < size_based_procs ? num_processes : size_based_procs ))
        current_destination="${local_output_dir}/${step_dir}/${TOKENIZER_NAME}"

        if [ -d "${current_destination}" ]; then
            echo "Output directory ${current_destination} already exists... Skipping ${source}"
            continue
        fi

        # tokenizing the source
        uv run dolma tokens \
            --documents "${local_input_dir}/${step_dir}/${EXTENSION}" \
            --destination "${local_output_dir}/${step_dir}/${TOKENIZER_NAME}" \
            --tokenizer.name_or_path ${TOKENIZER_NAME} \
            --tokenizer.eos_token_id 100257 \
            --tokenizer.pad_token_id 100277 \
            --fields.id_field_name 'commit' \
	    --no-tokenizer.segment_before_tokenization \
            --tokenizer.encode_special_tokens \
            --processes ${num_processes} \
            --max_size 4_000_000_000 \
            --sample_ring_prop \
            --dtype uint32
    done

    output_dir=$(echo "pretraining-data/sources/${source}" | sed 's|^pretraining-data/sources|preprocessed|')
    local_dir="${LOCAL_DIR}/${output_dir}"
    s3_dir="${REMOTE_DIR}/${output_dir}"

    if [ -d "${local_dir}" ]; then
        echo "Uploading ${source} to ${s3_dir}..."
        s5cmd cp -sp "${local_dir}/*" "${s3_dir}/"
    fi
done
