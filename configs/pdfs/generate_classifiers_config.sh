#!/bin/bash

# Script to generate PDF classifier configs.
# Generated config runs:
#   1. gzip tagging on text
#   2. finepdfish_dclm classifier (ultrafineweb + linear_transform) on text
#   3. finepdfish_edu classifier (ultrafineweb + linear_transform) on text
#   4. Geometric mean of the two combined classifier scores

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/classifiers"

mkdir -p "$OUTPUT_DIR"

export OUTPUT_DIR

S3_DCLM_BASE="s3://ai2-llm/classifiers/pdf-quality/trained_models/fasttext/finepdfish_dclm_ultrafine_bin5"
S3_EDU_BASE="s3://ai2-llm/classifiers/pdf-quality/trained_models/fasttext/finepdfish_edu_ultrafine_bin5"

echo "Fetching DCLM classifier calibration from S3..."
DCLM_CALIBRATION=$(aws s3 cp "${S3_DCLM_BASE}/calibration.yaml" - 2>/dev/null)

if [[ -z "$DCLM_CALIBRATION" ]]; then
    echo "ERROR: Could not fetch calibration.yaml for DCLM classifier"
    exit 1
fi

echo "Fetching EDU classifier calibration from S3..."
EDU_CALIBRATION=$(aws s3 cp "${S3_EDU_BASE}/calibration.yaml" - 2>/dev/null)

if [[ -z "$EDU_CALIBRATION" ]]; then
    echo "ERROR: Could not fetch calibration.yaml for EDU classifier"
    exit 1
fi

export DCLM_CALIBRATION EDU_CALIBRATION

uv run --with=pyyaml python3 - << 'PYEOF'
import os
import yaml

output_dir = os.environ["OUTPUT_DIR"]

DCLM_MODEL = (
    "/mnt/raid0/ai2-llm/classifiers/pdf-quality/trained_models/fasttext/"
    "finepdfish_dclm_ultrafine_bin5/model.bin"
)
EDU_MODEL = (
    "/mnt/raid0/ai2-llm/classifiers/pdf-quality/trained_models/fasttext/"
    "finepdfish_edu_ultrafine_bin5/model.bin"
)


def parse_calibration(calibration_env_var: str, field_prefix: str):
    calibration = yaml.safe_load(os.environ[calibration_env_var])
    weights = calibration.get("weights", {})
    component_names = calibration.get("component_names", sorted(weights.keys()))
    bias = calibration.get("bias", 0.0)

    features_lines = []
    for component_name in component_names:
        weight = weights[component_name]
        features_lines.append(f"              - field: {field_prefix}.{component_name}")
        features_lines.append(f"                weight: {weight}")

    return "\n".join(features_lines), bias


dclm_features_block, dclm_bias = parse_calibration(
    "DCLM_CALIBRATION", "metadata.finepdfish_dclm"
)
edu_features_block, edu_bias = parse_calibration(
    "EDU_CALIBRATION", "metadata.finepdfish_edu"
)

output_path = os.path.join(output_dir, "finepdfish.yaml")
with open(output_path, "w") as f:
    f.write(
        f"""\
name: finepdfish_pdf_classifier
text_field: text
pipeline:
    - name: gzip_annotator
      kwargs:
          text_field: text
          anno_field: metadata.gzip_compression_ratio

    - name: ultrafineweb_annotator
      kwargs:
          text_field: text
          output_field: metadata.finepdfish_dclm
          max_text_length: 10000
          fast_text_file: {DCLM_MODEL}
    - name: linear_transform_annotator
      kwargs:
          features:
{dclm_features_block}
          bias: {dclm_bias}
          output_field: metadata.finepdfish_dclm_combined

    - name: ultrafineweb_annotator
      kwargs:
          text_field: text
          output_field: metadata.finepdfish_edu
          max_text_length: 10000
          fast_text_file: {EDU_MODEL}
    - name: linear_transform_annotator
      kwargs:
          features:
{edu_features_block}
          bias: {edu_bias}
          output_field: metadata.finepdfish_edu_combined

    - name: jq_annotator
      kwargs:
          expression: '((.metadata.finepdfish_dclm_combined // 0 | [., 1] | min | [., 0] | max) * (.metadata.finepdfish_edu_combined // 0 | [., 1] | min | [., 0] | max)) | sqrt'
          output_field: metadata.combined_quality_score
"""
    )

print(f"Created {output_path}")
PYEOF
