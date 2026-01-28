cargo run --release range-partition \
  --input-dir /mnt/raid0/ai2-llm/pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii_qualitytagged/step_final/ \
  --output-dir /mnt/raid0/ai2-llm/pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii_qualitytagged/partitioned/ \
  --value metadata.ultrafineweb_quality.__label__pos \
  --default-value 0.0 \
  --range-groups "0.000575,0.001793,0.003922,0.007474,0.013133,0.021846,0.035269,0.055727,0.086276,0.130856,0.192578,0.277407,0.389369,0.524368,0.668067,0.804215,0.905691,0.966456,0.993577"  \
  --max-file-size 268435456 \
  --bucket-name "ufw"