languages=(
    "C"
    "C++"
    "C-Sharp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Python"
    "Rust"
    "SQL"
    "Shell"
    "Swift"
    "TypeScript"
)

source_dir="${HOME}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2/data"
output_dir="${HOME}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated"
error_dir="${HOME}/ai2-llm/pretraining-data/sources/the-stack-v2/spring2code_v2/minhash_v2_annotated_error"

for language in "${languages[@]}"; do
    if [ "$language" == "C++" ]; then
        model_name="whitespace-Cpp_lr05_ng3_n3M6"
    elif [ "$language" == "C-Sharp" ]; then
        model_name="whitespace-CSharp_lr05_ng3_n3M6"
    else
        model_name="whitespace-${language}_lr05_ng3_n3M6"
    fi

    cat examples/code-to-code/base.yaml | sed "s/MODEL_NAME_REPLACE_ME/${model_name}/g" > examples/code-to-code/${language}.yaml

    echo "Running $language"
    mkdir -p $output_dir/$language
    mkdir -p $error_dir/$language
    cargo run --release -- map \
        --config examples/code-to-code/$language.yaml \
        --input-dir $source_dir/$language \
        --output-dir $output_dir/$language \
        --err-dir $error_dir/$language
done
