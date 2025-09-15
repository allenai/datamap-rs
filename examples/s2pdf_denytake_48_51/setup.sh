
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
    --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2/${CATEGORY}/step_final" \
    --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted/${CATEGORY}/" \
    --config ./examples/s2orc_denytake_48_51/config.yaml
done

