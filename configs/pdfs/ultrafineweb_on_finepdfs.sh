#!/usr/bin/env bash

set -euox pipefail

cargo run --release map \
    --input-dir   "/mnt/raid0/ai2-llm/pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii/" \
    --output-dir  "/mnt/raid0/ai2-llm/pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii_qualitytagged/" \
    --config "/home/ec2-user/datamap-rs/configs/pdfs/ultrafineweb_on_finepdfs.yaml"

