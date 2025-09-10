X="transportation/vigintile_0005"
rm -rf /mnt/raid0/input
rm -rf /mnt/raid0/output
rm -rf /mnt/raid0/resharded
rm -rf /mnt/raid0/tokens 
rm -rf ~/d2

# Step 0: download
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/weborganizer_ft/dclm_plus2_vigintiles/data/${X}/*" /mnt/raid0/input/
cd /mnt/raid0/input/
ls -1 | tail -n 1 | xargs rm

# Step 1: reshard 
cd ~/datamap-rs 
cargo run --release -- reshard --input-dir /mnt/raid0/input --output-dir /mnt/raid0/resharded --max-size 256000000 --subsample 0.017

sudo yum install pip -y
pip install uv
cd
uv init d2
cd d2
uv add dolma
uv run dolma tokens     --documents "/mnt/raid0/resharded/*"     --destination /mnt/raid0/tokens/     --tokenizer.name_or_path 'allenai/dolma2-tokenizer'     --tokenizer.eos_token_id 100257     --tokenizer.pad_token_id 100277     --no-tokenizer.segment_before_tokenization     --tokenizer.encode_special_tokens     --ring_size 8     --processes 16     --max_size 4_000_000_000     --sample_ring_prop     --dtype 'uint32'


s5cmd cp -sp /mnt/raid0/resharded/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/vigintile_subsamples_0.017/data/${X}/"
s5cmd cp -sp /mnt/raid0/tokens/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/vigintile_subsamples_0.017/tokens/${X}/"