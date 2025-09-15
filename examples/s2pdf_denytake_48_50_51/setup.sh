

# We ran this, but it had excluded lengths < 2e12 (4096) so we now want to do it on the full pretraining set
cargo run --release -- map \
  --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_length-buckets_compression-decon-2" \
  --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_length-buckets_compression-decon-2_denyagain" \
  --config ./examples/s2pdf_denytake_48_50_51/config.yaml

cargo run --release -- map \
  --input-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard" \
  --output-dir "/mnt/raid0/s2pdf_dedupe_minhash_v1_with_no_pii_basic_quality_datadelve_norefs_mdtables_v2_denylisted_reshard_denyagain" \
  --config ./examples/s2pdf_denytake_48_50_51/config.yaml

