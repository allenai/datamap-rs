#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zstandard",
#     "tqdm",
#     "orjson",
# ]
# ///
"""Assign metadata.dedupe_priority based on field presence.

Rules (applied per document):
  - "fos" key exists          → priority "0"
  - metadata.weborganizer exists → priority "1"
  - otherwise                  → priority "2"

Reads .jsonl.zst files from --input-dir, writes modified copies to --output-dir
preserving the relative directory structure.
"""

import argparse
import io
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import orjson
import zstandard as zstd
from tqdm import tqdm

NUM_WORKERS = os.cpu_count()


def find_jsonl_zst_files(base_dir: str) -> list[str]:
    """Recursively find all .jsonl.zst files under a directory."""
    files = []
    for root, _, filenames in os.walk(base_dir):
        for fn in filenames:
            if fn.endswith(".jsonl.zst"):
                files.append(os.path.join(root, fn))
    return sorted(files)


def process_file(args: tuple[str, str, str]) -> tuple[int, int, int, int]:
    """Read a .jsonl.zst, assign dedupe_priority, write to output.

    Returns (total_docs, priority_0, priority_1, priority_2).
    """
    filepath, source_base, out_base = args
    rel = os.path.relpath(filepath, source_base)
    out_path = os.path.join(out_base, rel)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    total = 0
    counts = [0, 0, 0]
    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor()

    try:
        with open(filepath, "rb") as fh_in, open(out_path, "wb") as fh_out:
            with cctx.stream_writer(fh_out) as writer:
                with dctx.stream_reader(fh_in) as reader:
                    for line in io.BufferedReader(reader):
                        if not line.strip():
                            continue
                        total += 1
                        try:
                            doc = orjson.loads(line)
                        except Exception:
                            writer.write(line)
                            continue

                        if "fos" in doc:
                            priority = "0"
                        elif doc.get("metadata", {}).get("weborganizer") is not None:
                            priority = "1"
                        else:
                            priority = "2"

                        counts[int(priority)] += 1

                        meta = doc.get("metadata")
                        if meta is None:
                            meta = {}
                            doc["metadata"] = meta
                        meta["dedupe_priority"] = priority

                        writer.write(orjson.dumps(doc) + b"\n")
    except Exception as e:
        print(f"\nError processing {filepath}: {e}", file=sys.stderr)

    return total, counts[0], counts[1], counts[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assign metadata.dedupe_priority based on field presence."
    )
    parser.add_argument("--input-dir", required=True, help="Input directory with .jsonl.zst files")
    parser.add_argument("--output-dir", required=True, help="Output directory for modified files")
    parser.add_argument(
        "--debug-n-files",
        type=int,
        default=None,
        metavar="N",
        help="Debug mode: only process the first N files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    if args.debug_n_files is not None:
        print(f"*** DEBUG MODE: processing at most {args.debug_n_files} files ***\n")

    files = find_jsonl_zst_files(input_dir)
    if args.debug_n_files is not None:
        files = files[: args.debug_n_files]

    print(f"Found {len(files):,} .jsonl.zst files in {input_dir}")

    tasks = [(f, input_dir, output_dir) for f in files]

    total_docs = 0
    total_p0 = 0
    total_p1 = 0
    total_p2 = 0

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = {pool.submit(process_file, t): t for t in tasks}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            docs, p0, p1, p2 = fut.result()
            total_docs += docs
            total_p0 += p0
            total_p1 += p1
            total_p2 += p2

    print(f"\nDone. {total_docs:,} documents processed.")
    print(f"  Priority 0 (has fos):              {total_p0:>12,}")
    print(f"  Priority 1 (has weborganizer):     {total_p1:>12,}")
    print(f"  Priority 2 (neither):              {total_p2:>12,}")
    print(f"\nOutput written to {output_dir}")


if __name__ == "__main__":
    main()
