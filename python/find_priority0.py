#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zstandard",
#     "orjson",
# ]
# ///
"""Scan files in a directory for any priority 0 docs. Prints the first match and exits."""

import io
import os
import sys

import orjson
import zstandard as zstd


def main():
    base = "/mnt/raid0/url_and_mh_grouped_priority_filtered"
    files = sorted(
        os.path.join(base, f)
        for f in os.listdir(base)
        if f.endswith(".jsonl.zst")
    )
    print(f"Scanning {len(files)} files...")

    dctx = zstd.ZstdDecompressor()
    for i, fpath in enumerate(files):
        if i % 500 == 0:
            print(f"  [{i}/{len(files)}] {os.path.basename(fpath)}", file=sys.stderr)
        lineno = 0
        with open(fpath, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                for line in io.BufferedReader(reader):
                    if not line.strip():
                        continue
                    lineno += 1
                    doc = orjson.loads(line)
                    p = doc.get("metadata", {}).get("dedupe_priority")
                    if p == "0":
                        print(f"\nFOUND priority 0 doc!")
                        print(f"  File: {fpath}")
                        print(f"  Line: {lineno}")
                        print(f"  id: {doc.get('id')}")
                        print(f"  fos_max: {doc.get('fos_max')}")
                        print(f"  has fos key: {'fos' in doc}")
                        sys.exit(0)

    print("\nNo priority 0 docs found in any file.")


if __name__ == "__main__":
    main()
