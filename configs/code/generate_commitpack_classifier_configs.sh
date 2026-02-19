#!/bin/bash

# Script to generate commitpack classifier configs for each language.
# Each config runs:
#   1. gzip tagging on new_contents
#   2. Language-specific code quality classifier (ultrafineweb + linear_transform) on new_contents
#   3. Global commit message quality classifier (ultrafineweb + linear_transform) on message
#   4. Geometric mean of code quality and commit message quality scores
#   5. Concatenate old_contents, message, and new_contents into text field

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/classifiers_commitpack"

mkdir -p "$OUTPUT_DIR"

export SCRIPT_DIR OUTPUT_DIR

S3_COMMIT_MSG_BASE="s3://ai2-llm/classifiers/code-quality/trained_models/fasttext/commitpack_commit_message_ultrafine_commits_bin5"

echo "Fetching commit message classifier calibration from S3..."
COMMIT_MSG_CALIBRATION=$(aws s3 cp "${S3_COMMIT_MSG_BASE}/calibration.yaml" - 2>/dev/null)

if [[ -z "$COMMIT_MSG_CALIBRATION" ]]; then
    echo "ERROR: Could not fetch calibration.yaml for commit message classifier"
    exit 1
fi

export COMMIT_MSG_CALIBRATION

uv run --with=pyyaml python3 - << 'PYEOF'
import os
import yaml

script_dir = os.environ['SCRIPT_DIR']
output_dir = os.environ['OUTPUT_DIR']

COMMIT_MSG_MODEL = (
    "/mnt/raid0/ai2-llm/classifiers/code-quality/trained_models/fasttext/"
    "commitpack_commit_message_ultrafine_commits_bin5/model.bin"
)

# Parse commit message classifier calibration
commit_cal = yaml.safe_load(os.environ['COMMIT_MSG_CALIBRATION'])
commit_weights = commit_cal.get('weights', {})
commit_component_names = commit_cal.get('component_names', sorted(commit_weights.keys()))
commit_bias = commit_cal.get('bias', 0.0)

# Build commit message calibration YAML block
commit_features_lines = []
for name in commit_component_names:
    w = commit_weights[name]
    commit_features_lines.append(f"              - field: metadata.stack_edu_commit_message.{name}")
    commit_features_lines.append(f"                weight: {w}")
commit_features_block = "\n".join(commit_features_lines)

# Mapping: commitpack language name -> (output config name, source classifier config)
LANG_MAP = {
    "bluespec": ("bluespec", "classifiers2/bluespec.yaml"),
    "c": ("c", "classifiers/c.yaml"),
    "c#": ("csharp", "classifiers/csharp.yaml"),
    "c++": ("cpp", "classifiers/cpp.yaml"),
    "clojure": ("clojure", "classifiers2/clojure.yaml"),
    "common-lisp": ("common_lisp", "classifiers2/common_lisp.yaml"),
    "css": ("css", "classifiers2/css.yaml"),
    "cuda": ("cuda", "classifiers2/cuda.yaml"),
    "dart": ("dart", "classifiers2/dart.yaml"),
    "erlang": ("erlang", "classifiers2/erlang.yaml"),
    "fortran": ("fortran", "classifiers2/fortran.yaml"),
    "go": ("go", "classifiers/go.yaml"),
    "haskell": ("haskell", "classifiers2/haskell.yaml"),
    "html": ("html", "classifiers2/html.yaml"),
    "java": ("java", "classifiers/java.yaml"),
    "java-server-pages": ("java_server_pages", "classifiers2/java_server_pages.yaml"),
    "javascript": ("javascript", "classifiers/javascript.yaml"),
    "julia": ("julia", "classifiers2/julia.yaml"),
    "jupyter-notebook": ("jupyter_notebook", "classifiers2/jupyter_notebook.yaml"),
    "kotlin": ("kotlin", "classifiers2/kotlin.yaml"),
    "lua": ("lua", "classifiers2/lua.yaml"),
    "markdown": ("markdown", "classifiers/markdown.yaml"),
    "mathematica": ("mathematica", "classifiers2/mathematica.yaml"),
    "matlab": ("matlab", "classifiers2/matlab.yaml"),
    "objective-c++": ("objective_cpp", "classifiers2/objective_c.yaml"),
    "ocaml": ("ocaml", "classifiers2/ocaml.yaml"),
    "opencl": ("opencl", "classifiers2/opencl.yaml"),
    "pascal": ("pascal", "classifiers2/pascal.yaml"),
    "perl": ("perl", "classifiers2/perl.yaml"),
    "php": ("php", "classifiers/php.yaml"),
    "python": ("python", "classifiers/python.yaml"),
    "r": ("r", "classifiers2/r.yaml"),
    "restructuredtext": ("restructuredtext", "classifiers2/restructuredtext.yaml"),
    "rmarkdown": ("rmarkdown", "classifiers2/rmarkdown.yaml"),
    "ruby": ("ruby", "classifiers/ruby.yaml"),
    "rust": ("rust", "classifiers/rust.yaml"),
    "scala": ("scala", "classifiers2/scala.yaml"),
    "scheme": ("scheme", "classifiers2/scheme.yaml"),
    "scss": ("scss", "classifiers2/scss.yaml"),
    "shell": ("shell", "classifiers/shell.yaml"),
    "sql": ("sql", "classifiers/sql.yaml"),
    "swift": ("swift", "classifiers/swift.yaml"),
    "systemverilog": ("systemverilog", "classifiers2/systemverilog.yaml"),
    "tcl": ("tcl", "classifiers2/tcl.yaml"),
    "typescript": ("typescript", "classifiers/typescript.yaml"),
    "vhdl": ("vhdl", "classifiers2/vhdl.yaml"),
    "vue": ("vue", "classifiers2/vue.yaml"),
}

count = 0
for commitpack_lang, (config_name, source_rel) in sorted(LANG_MAP.items()):
    source_path = os.path.join(script_dir, source_rel)

    if not os.path.exists(source_path):
        print(f"  WARNING: Source config not found: {source_path}")
        continue

    with open(source_path) as f:
        config = yaml.safe_load(f)

    # Extract ultrafineweb_annotator and linear_transform_annotator from source
    model_path = None
    features = []
    bias = 0.0
    output_field = "metadata.stack_edu_redux_combined"

    for step in config.get("pipeline", []):
        if step["name"] == "ultrafineweb_annotator":
            model_path = step["kwargs"]["fast_text_file"]
        elif step["name"] == "linear_transform_annotator":
            kwargs = step["kwargs"]
            features = kwargs["features"]
            bias = kwargs["bias"]
            output_field = kwargs["output_field"]

    if not model_path or not features:
        print(f"  WARNING: Missing annotator data in {source_rel}")
        continue

    # Build features YAML block
    features_lines = []
    for feat in features:
        features_lines.append(f"              - field: {feat['field']}")
        features_lines.append(f"                weight: {feat['weight']}")
    features_block = "\n".join(features_lines)

    output_path = os.path.join(output_dir, f"{config_name}.yaml")
    with open(output_path, "w") as f:
        f.write(f"""\
name: {config_name}_commitpack_classifier
pipeline:
    - name: gzip_annotator
      kwargs:
          text_field: new_contents
          anno_field: metadata.gzip_compression_ratio

    - name: ultrafineweb_annotator
      kwargs:
          text_field: new_contents
          output_field: metadata.stack_edu_redux
          max_text_length: 10000
          fast_text_file: {model_path}
    - name: linear_transform_annotator
      kwargs:
          features:
{features_block}
          bias: {bias}
          output_field: {output_field}

    - name: ultrafine_commit_annotator
      kwargs:
          text_field: message
          output_field: metadata.stack_edu_commit_message
          max_text_length: 10000
          fast_text_file: {COMMIT_MSG_MODEL}
    - name: linear_transform_annotator
      kwargs:
          features:
{commit_features_block}
          bias: {commit_bias}
          output_field: metadata.stack_edu_commit_message_combined

    - name: jq_annotator
      kwargs:
          expression: '((.metadata.stack_edu_redux_combined // 0 | [., 1] | min | [., 0] | max) * (.metadata.stack_edu_commit_message_combined // 0 | [., 1] | min | [., 0] | max)) | sqrt'
          output_field: metadata.combined_quality_score

    - name: jq_annotator
      kwargs:
          expression: '((.old_contents // "") | gsub("^\\\\s+|\\\\s+$"; "")) + "\\n\\n\\n\\n" + ((.message // "") | gsub("^\\\\s+|\\\\s+$"; "")) + "\\n\\n\\n\\n" + ((.new_contents // "") | gsub("^\\\\s+|\\\\s+$"; ""))'
          output_field: text
""")

    print(f"  Created {output_path}")
    count += 1

print(f"\nDone! Generated classifier configs for {count} languages.")
PYEOF
