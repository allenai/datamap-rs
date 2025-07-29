#!/bin/bash

CATEGORIES=(
  adult
  art_design
  crime_law
  education_jobs
  entertainment
  fashion_beauty
  finance_business
  food_dining
  games
  hardware
  health
  history
  home_hobbies
  industrial
  literature
  politics
  religion
  science_tech
  social_life
  software
  software_dev
  sports_fitness
  transportation
  travel
)

# Loop through pdf_quality values from 0000 to 0006
for PDF_QUALITY in {0000..0006}; do
  echo "Processing PDF quality: $PDF_QUALITY"
  
  for CATEGORY in "${CATEGORIES[@]}"; do
    echo "  Processing $CATEGORY..."
    cargo run --release -- \
          partition-by-length \
          --input-dir /mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_quality_tagged_partitioned/${CATEGORY}/pdf_quality_${PDF_QUALITY}/ \
          --output-dir /mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_quality_tagged_partitioned_length/${CATEGORY}/pdf_quality_${PDF_QUALITY}/
  done
  
  echo "Completed PDF quality: $PDF_QUALITY"
done

echo "All processing completed!"