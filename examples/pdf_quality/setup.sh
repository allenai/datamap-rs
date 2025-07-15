#!/bin/bash

set -e

# Before running, grab your PDF data, usually from s3://ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted/
# and store it to /mnt/raid0
#s5cmd cp -sp s3://ai2-llm/pretraining-data/sources/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted/* /mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted/

# Download the quality filter
if [ ! -f "/mnt/raid0/models/ft_pdf_quality_lr05_wng3_minn3_maxn6.bin" ]; then
    s5cmd cp -sp s3://ai2-llm/models/fasttext_pdf_quality_classifiers/ft_pdf_quality_lr05_wng3_minn3_maxn6* /mnt/raid0/models/
fi

# You can set the bounds on the float filter to decide how much you want
# It tends to roughly match, 0.5 is around 50% of the data 0.75 is ~25%

# cargo run --release -- map \
#     --input-dir "/mnt/raid0/s2pdf_mini" \
#     --output-dir "/mnt/raid0/s2pdf_mini_tagged" \
#     --config ./examples/pdf_quality/config.yaml


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

for CATEGORY in "${CATEGORIES[@]}"; do
  echo "Processing $CATEGORY..."
  cargo run --release -- map \
    --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted/${CATEGORY}/step_final/step_final" \
    --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_quality_tagged/${CATEGORY}/" \
    --config ./examples/pdf_quality/config.yaml
done