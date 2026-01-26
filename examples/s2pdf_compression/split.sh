CONFIGS=(
    p00
)


# CATEGORIES=(
#   adult
# )
# CATEGORIES=(
#   adult
#   art_design
#   crime_law
#   education_jobs
#   entertainment
#   fashion_beauty
#   finance_business
#   food_dining
#   games
#   hardware
#   health
#   history
#   home_hobbies
#   industrial
#   literature
#   politics
#   religion
#   science_tech
#   social_life
#   software
#   software_dev
#   sports_fitness
#   transportation
#   travel
# )


for CONFIG in "${CONFIGS[@]}"; do

    cargo run --release -- map \
        --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain/step_final/$CATEGORY" \
        --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain_compressionv2/$CONFIG" \
        --config ./examples/s2pdf_compression/$CONFIG.yaml

done


#in the end we decided to just stick with 10th to 90th percentiles
cargo run --release -- map \
        --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain/step_final/" \
        --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain_compressionv2/" \
        --config ./examples/s2pdf_compression/10_to_90.yaml
