X=17
rm -rf /mnt/raid0/input/*
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/minhash/param_26_11/sorted/${X}/*" "/mnt/raid0/input/${X}/"
cd ~/datamap-rs
git checkout sort_v2
git pull
cargo run --release -- sorted-dupaware --input-dir /mnt/raid0/input --output-dir /mnt/raid0/output --dupkey metadata.minhash.cc_id --subsample 0.04 --max-cc-size 100 > "/mnt/raid0/dupaware_${X}.log"
cargo run --release -- reshard --input-dir /mnt/raid0/output --output-dir /mnt/raid0/resharded --max-size 256000000 --full-cat 

s5cmd cp -sp /mnt/raid0/resharded/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/dedup_ablations_v2/base_dupaware_0.04/data/${X}/"
s5cmd cp -sp "/mnt/raid0/dupaware_${X}.log" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/dedup_ablations_v2/base_dupaware_0.04/logs"