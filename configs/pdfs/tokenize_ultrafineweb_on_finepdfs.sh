huggingface-cli download allenai/dolma2-tokenizer --local-dir /mnt/raid0/dolma2-tokenizer

for split in /mnt/raid0/ai2-llm/pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii_qualitytagged/partitioned/ufw_*; do                                                            
      name=$(basename "$split")                                                                                                                                                                    
      echo "Processing $name..."                                                                                                                                                                   
      dolma tokens \
          --documents "${split}/*" \
          --destination "/mnt/raid0/tokenized/${name}" \
          --tokenizer.name_or_path /mnt/raid0/dolma2-tokenizer/tokenizer.json \
          --tokenizer.eos_token_id 100257 \
          --tokenizer.pad_token_id 100277 \
          --no-tokenizer.segment_before_tokenization \
          --tokenizer.encode_special_tokens \
          --processes $(python3 -c "import multiprocessing; print(multiprocessing.cpu_count())") \
          --max_size 4_000_000_000 \
          --sample_ring_prop \
          --dtype uint32                                                                                                                                                                           
  done    

for split in /mnt/raid0/tokenized; do
    s5cmd cp "${split}/" s3://ai2-llm/preprocessed/HuggingFaceFW_finepdfs/allenai/dolma2-tokenizer/
done