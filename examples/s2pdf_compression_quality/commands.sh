
# Needs to be on upsample-tools branch...

# Build reservoir sample
#cargo run --release -- reservoir-sample --input-dir /mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain --output-dir /mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain_compression.reservoir --config ~/datamap-rs/examples/s2pdf_compression_quality/config.yaml 

# Now, for each segment of data, split it up
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

# Function to process a single category
process_category() {
  local CATEGORY=$1
  echo "Processing $CATEGORY..."
  
  cargo run --release -- percentile-partition \
   --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain/step_final/${CATEGORY}" \
   --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain_compression/${CATEGORY}/" \
   --config ~/datamap-rs/examples/s2pdf_compression_quality/config.yaml \
   --reservoir-path /mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain_compression.reservoir
}

# Process categories in parallel, up to 4 at a time
MAX_PARALLEL=4
count=0

for CATEGORY in "${CATEGORIES[@]}"; do
  # Start the process in background
  process_category "$CATEGORY" &
  
  # Increment counter
  ((count++))
  
  # If we've reached MAX_PARALLEL, wait for one to finish
  if [ $count -ge $MAX_PARALLEL ]; then
    wait -n  # Wait for any background job to finish
    ((count--))
  fi
done

# Wait for all remaining jobs to complete
wait
echo "All categories processed."