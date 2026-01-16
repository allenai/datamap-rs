mkdir -p ft_classifiers
mkdir -p tokenizers
curl -L -o tokenizers/deepseek_v2.json https://huggingface.co/deepseek-ai/DeepSeek-V2/raw/main/tokenizer.json


# Python
s5cmd cp -sp s3://ai2-llm/classifiers/code-quality/trained_models/fasttext/the-stack-v2_spring2code_v2_minhash_v2_annotated_sample_1GB_countup_criteria_v2_gpt-5-mini_10k_trimmed_fasttext_ultrafine-plus-code_thr13/Python/model.bin ft_classifiers/code_classifier_python.bin
