X="history_and_geography/vigintile_0006"

rm -rf /mnt/raid0/input
rm -rf /mnt/raid0/tokens 
rm -rf ~/d2
sudo yum install pip -y
pip install uv
cd
uv init d2
cd d2
uv add dolma
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/weborganizer_ft/dclm_plus2_vigintiles/data/${X}/*" /mnt/raid0/input/
uv run dolma tokens     --documents "/mnt/raid0/input/*"     --destination /mnt/raid0/tokens/     --tokenizer.name_or_path 'allenai/dolma2-tokenizer'     --tokenizer.eos_token_id 100257     --tokenizer.pad_token_id 100277     --no-tokenizer.segment_before_tokenization     --tokenizer.encode_special_tokens     --ring_size 8     --processes 127     --max_size 4_000_000_000     --sample_ring_prop     --dtype 'uint32'

s5cmd cp -sp /mnt/raid0/tokens/ "s3://ai2-llm/preprocessed/cc_all_dressed/all_dressed_v3/dclm_plus2_vigilantes/allenai/dolma2-tokenizer/${X}/"