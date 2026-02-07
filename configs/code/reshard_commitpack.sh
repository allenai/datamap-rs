#!/usr/bin/env bash

LOCAL_PREFIX=${LOCAL_PREFIX:-"/mnt/raid0/ai2-llm"}
REMOTE_PREFIX=${REMOTE_PREFIX:-"s3://ai2-llm"}

INPUT_DIR=${INPUT_DIR:-"pretraining-data/sources/bigcode_commitpack/raw/data"}
OUTPUT_DIR=${OUTPUT_DIR:-"pretraining-data/sources/bigcode_commitpack/dolma-3_5-languages"}


LANGUAGES=(
    "bluespec"
    "c"
    "c#"
    "c++"
    "clojure"
    "common-lisp"
    "css"
    "cuda"
    "dart"
    "erlang"
    "fortran"
    "go"
    "haskell"
    "html"
    "java"
    "java-server-pages"
    "javascript"
    "julia"
    "jupyter-notebook"
    "kotlin"
    "lua"
    "markdown"
    "mathematica"
    "matlab"
    "objective-c++"
    "ocaml"
    "opencl"
    "pascal"
    "perl"
    "php"
    "python"
    "r"
    "restructuredtext"
    "rmarkdown"
    "ruby"
    "rust"
    "scala"
    "scheme"
    "scss"
    "shell"
    "sql"
    "swift"
    "systemverilog"
    "tcl"
    "typescript"
    "vhdl"
    "vue"
)

for language in "${LANGUAGES[@]}"; do
    echo "Resharding ${language}..."

    remote_input_dir="${REMOTE_PREFIX}/${INPUT_DIR}/${language}"
    local_input_dir="${LOCAL_PREFIX}/${INPUT_DIR}/${language}"
    local_output_dir="${LOCAL_PREFIX}/${OUTPUT_DIR}/${language}"
    remote_output_dir="${REMOTE_PREFIX}/${OUTPUT_DIR}/${language}"

    s5cmd cp -sp "${remote_input_dir}/*" "${local_input_dir}/"

    cargo run --release reshard \
        --input-dir ${local_input_dir} \
        --output-dir ${local_output_dir} \
        --max-size 100000000

    s5cmd cp -sp "${local_output_dir}/*" "${remote_output_dir}/"
done

echo "Done!"
