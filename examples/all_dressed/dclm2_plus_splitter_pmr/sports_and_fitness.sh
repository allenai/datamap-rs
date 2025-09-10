X="sports_and_fitness"
rm -rf /mnt/raid0/input/*
rm -rf /mnt/raid0/output/*
s5cmd cp -sp "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/weborganizer_ft/base/${X}/*" /mnt/raid0/input/
cd datamap-rs
git checkout upsample-tools 
git pull
cargo run --release -- full-percentile-partition --input-dir /mnt/raid0/input/ --output-dir /mnt/raid0/output/ --config examples/all_dressed/upsample_splitter.yaml > "/mnt/raid0/${X}.log"
s5cmd cp -sp /mnt/raid0/output/ "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/weborganizer_ft/dclm_plus2_vigintiles/data/${X}/"
s5cmd cp -sp "/mnt/raid0/${X}.log" "s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/weborganizer_ft/dclm_plus2_vigintiles/logs/${X}.log" 