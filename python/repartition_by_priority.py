#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zstandard",
#     "tqdm",
#     "orjson",
# ]
# ///
"""Repartition data by dedupe_priority into separate output folders.

Priority routing:
  - "2" → {output}/finepdfs/{chunk_name}
  - "0" → {output}/s2orcforolmo/{fos_max}/{chunk_name}
  - "1" → {output}/olmo-crawled-pdfs/{wo_topic}/{chunk_name}

Each sub-folder ends up with up to 16384 chunks (one per input file).
"""

import argparse
import io
import os
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import zstandard as zstd
from tqdm import tqdm

NUM_WORKERS = os.cpu_count()


def find_jsonl_zst_files(base_dir: str) -> list[str]:
    files = []
    for fn in os.listdir(base_dir):
        if fn.endswith(".jsonl.zst"):
            files.append(os.path.join(base_dir, fn))
    return sorted(files)


def process_file(args: tuple[str, str, str]) -> dict:
    """Read one chunk, route docs by priority, write to output folders.

    Returns stats dict with counts.
    """
    filepath, source_base, out_base = args
    fname = os.path.basename(filepath)

    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor()

    # Buffer lines by output relative path
    buffers: dict[str, list[bytes]] = defaultdict(list)

    total = 0
    counts = {"0": 0, "1": 0, "2": 0, "other": 0}

    try:
        with open(filepath, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                for line in io.BufferedReader(reader):
                    if not line.strip():
                        continue
                    total += 1
                    try:
                        doc = orjson.loads(line)
                    except Exception:
                        continue

                    priority = doc.get("metadata", {}).get("dedupe_priority", "2")

                    if priority == "2":
                        out_rel = os.path.join("finepdfs", fname)
                    elif priority == "0":
                        fos = doc.get("fos_max", "unknown")
                        out_rel = os.path.join("s2orcforolmo", fos, fname)
                    elif priority == "1":
                        wo_topic = doc.get("metadata", {}).get("wo_topic", "unknown")
                        out_rel = os.path.join("olmo-crawled-pdfs", wo_topic, fname)
                    else:
                        out_rel = os.path.join("finepdfs", fname)
                        priority = "other"

                    counts[priority] = counts.get(priority, 0) + 1
                    buffers[out_rel].append(orjson.dumps(doc) + b"\n")
    except Exception as e:
        print(f"\nError reading {filepath}: {e}", file=sys.stderr)
        return {"total": 0, "0": 0, "1": 0, "2": 0, "other": 0, "files_written": 0}

    # Write buffered output
    files_written = 0
    for out_rel, lines in buffers.items():
        out_path = os.path.join(out_base, out_rel)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        try:
            with open(out_path, "wb") as fh_out:
                with cctx.stream_writer(fh_out) as writer:
                    for line_bytes in lines:
                        writer.write(line_bytes)
            files_written += 1
        except Exception as e:
            print(f"\nError writing {out_path}: {e}", file=sys.stderr)

    return {
        "total": total,
        "0": counts["0"],
        "1": counts["1"],
        "2": counts["2"],
        "other": counts["other"],
        "files_written": files_written,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repartition data by dedupe_priority into separate output folders."
    )
    parser.add_argument(
        "--input-dir",
        default="/mnt/raid0/url_and_mh_grouped_priority_filtered",
        help="Input directory with .jsonl.zst files",
    )
    parser.add_argument("--output-dir", required=True, help="Output base directory")
    parser.add_argument(
        "--debug-n-files",
        type=int,
        default=None,
        metavar="N",
        help="Debug mode: only process the first N files",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=NUM_WORKERS,
        help=f"Number of parallel workers (default: {NUM_WORKERS})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    files = find_jsonl_zst_files(input_dir)
    if args.debug_n_files is not None:
        print(f"*** DEBUG MODE: processing at most {args.debug_n_files} files ***\n")
        files = files[: args.debug_n_files]

    print(f"Found {len(files):,} .jsonl.zst files in {input_dir}")
    print(f"Using {args.workers} workers")
    print(f"Output: {output_dir}")
    print()

    tasks = [(f, input_dir, output_dir) for f in files]

    totals = {"total": 0, "0": 0, "1": 0, "2": 0, "other": 0, "files_written": 0}

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_file, t): t for t in tasks}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Repartitioning"):
            result = fut.result()
            for k in totals:
                totals[k] += result[k]

    print(f"\nDone. {totals['total']:,} documents processed.")
    print(f"  Priority 0 (s2orcforolmo):         {totals['0']:>12,}")
    print(f"  Priority 1 (olmo-crawled-pdfs):    {totals['1']:>12,}")
    print(f"  Priority 2 (finepdfs):             {totals['2']:>12,}")
    if totals["other"]:
        print(f"  Other/unknown:                     {totals['other']:>12,}")
    print(f"  Output files written:              {totals['files_written']:>12,}")
    print(f"\nOutput written to {output_dir}")


if __name__ == "__main__":
    main()
