mkdir -p ft_classifiers
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json

s5cmd sync 's3://ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5/*' '/mnt/raid0/ai2-llm/classifiers/code-quality/trained_models/fasttext/stack_edu_redux_ultrafine_bin5/'
