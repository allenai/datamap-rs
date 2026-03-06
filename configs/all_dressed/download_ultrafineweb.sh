mkdir -p ft_classifiers
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json
curl -L -o ft_classifiers/ultrafineweb.bin https://huggingface.co/openbmb/Ultra-FineWeb-classifier/resolve/main/classifiers/ultra_fineweb_en.bin

