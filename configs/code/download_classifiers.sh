mkdir -p ft_classifiers
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json

s5cmd cp -sp 's3://ai2-llm/classifiers/code-quality/trained_models/fasttext/*' '/mnt/raid0/ai2-llm/classifiers/code-quality/trained_models/fasttext/'
