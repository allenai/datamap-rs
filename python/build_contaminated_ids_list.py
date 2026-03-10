#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["tqdm"]
# ///
"""
Build a list of contaminated document IDs from decontamination reports.

Scans a source folder for *_reports directories, reads the JSONL report files
inside, and looks up the corresponding training lines to extract document IDs.
"""

import argparse
import gzip
import json
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm


def find_report_dirs(source_dir: str) -> list[Path]:
    """Recursively find all directories ending with _reports."""
    report_dirs = []
    for root, dirs, _ in os.walk(source_dir):
        for d in dirs:
            if d.endswith("_reports"):
                report_dirs.append(Path(root) / d)
    return report_dirs


def collect_line_numbers(report_jsonl: Path) -> tuple[str | None, set[int]]:
    """Read a report JSONL and return (training_file, set of line numbers).

    All lines in a single report JSONL reference the same training_file,
    so we only read training_file from the first entry.
    """
    training_file = None
    line_numbers = set()
    with open(report_jsonl, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if training_file is None:
                training_file = entry["training_file"]
            line_numbers.add(entry["training_line"])
    return training_file, line_numbers


def extract_ids_from_shard(shard_path: Path, line_numbers: set[int]) -> list[str]:
    """Read specific line numbers from a shard JSONL(.gz) and return their IDs."""
    ids = []
    max_line = max(line_numbers)
    opener = gzip.open if shard_path.suffix == ".gz" else open
    with opener(shard_path, "rt") as f:
        for line_idx, line in enumerate(f):
            if line_idx in line_numbers:
                entry = json.loads(line)
                ids.append(entry["id"])
                if line_idx >= max_line:
                    break
    return ids


def _process_shard(args: tuple[Path, set[int]]) -> list[str]:
    """Worker function for ProcessPoolExecutor."""
    shard_path, line_numbers = args
    return extract_ids_from_shard(shard_path, line_numbers)


def main():
    parser = argparse.ArgumentParser(
        description="Build a list of contaminated document IDs from decontamination reports."
    )
    parser.add_argument("source_dir", help="Root directory to scan for *_reports folders")
    parser.add_argument("-o", "--output", required=True, help="Output .txt file for IDs")
    parser.add_argument("-w", "--workers", type=int, default=None, help="Number of worker processes (default: cpu count)")
    args = parser.parse_args()

    report_dirs = find_report_dirs(args.source_dir)
    print(f"Found {len(report_dirs)} report directories")

    # Map: data_dir -> { training_file -> set of line numbers }
    lookups: dict[Path, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))

    for report_dir in tqdm(report_dirs, desc="Scanning reports"):
        # The data directory is the same path with _reports stripped
        data_dir = report_dir.parent / report_dir.name.removesuffix("_reports")

        report_files = sorted(report_dir.rglob("*.jsonl"))
        for rf in report_files:
            training_file, line_numbers = collect_line_numbers(rf)
            if training_file and line_numbers:
                lookups[data_dir][training_file].update(line_numbers)

    # Build work items: (shard_path, line_numbers)
    work_items = []
    for data_dir, file_map in sorted(lookups.items()):
        for training_file, line_numbers in sorted(file_map.items()):
            shard_path = data_dir / training_file
            if not shard_path.exists():
                # Try .gz variant
                shard_path = data_dir / (training_file + ".gz")
            if not shard_path.exists():
                print(f"WARNING: {data_dir / training_file}(.gz) not found, skipping")
                continue
            work_items.append((shard_path, line_numbers))

    print(f"Reading IDs from {len(work_items)} shards using {args.workers or os.cpu_count()} workers")

    # Extract IDs in parallel
    all_ids = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_process_shard, item): item[0] for item in work_items}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting IDs"):
            all_ids.extend(future.result())

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for doc_id in all_ids:
            f.write(doc_id + "\n")

    print(f"Wrote {len(all_ids)} contaminated IDs to {args.output}")


if __name__ == "__main__":
    main()
