#!/usr/bin/env python3
"""
Generate an S3 Batch Operations manifest (CSV) listing all *.npy and *.csv.gz
files referenced by the "Npy paths" column in an input CSV.

Runs all s5cmd ls commands in parallel for speed.

Output format: Bucket,Key  (one row per object, no header — ready for S3 Batch Ops).

Usage:
    python gen_manifest.py <csv_path> [-o manifest.csv] [--workers N]
"""

import argparse
import csv
import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

EXTENSIONS = ("*.npy", "*.csv.gz")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv_path", help="Path to the input CSV file")
    p.add_argument(
        "-o", "--output",
        default="manifest.csv",
        help="Output manifest path (default: manifest.csv)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Parallel s5cmd ls invocations (default: 32)",
    )
    return p.parse_args()


def read_patterns(csv_path: str) -> list[dict]:
    """Build {name, patterns: [s3 glob, ...], bucket} for each CSV row."""
    entries = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Name", "").strip()
            npy_path = row.get("Npy paths", "").strip()
            if not npy_path:
                continue

            parsed = urlparse(npy_path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")

            # Derive patterns for each extension.
            # Most paths look like .../allenai/dolma2-tokenizer/*.npy
            # A few are bare directories (no glob) — append allenai/dolma2-tokenizer/<ext>
            patterns = []
            if key.endswith("*.npy"):
                base = key[: -len("*.npy")]  # keeps trailing /
                for ext in EXTENSIONS:
                    patterns.append(f"s3://{bucket}/{base}{ext}")
            elif key.endswith("/"):
                # Bare directory — assume allenai/dolma2-tokenizer/ structure
                base = key + "allenai/dolma2-tokenizer/"
                for ext in EXTENSIONS:
                    patterns.append(f"s3://{bucket}/{base}{ext}")
            else:
                log.warning("Unexpected Npy path format for %r: %s", name, npy_path)
                continue

            entries.append({"name": name, "bucket": bucket, "patterns": patterns})
    return entries


def _prefix_for(pattern: str) -> str:
    """Everything up to and including the last '/' before the first wildcard."""
    first_wild = len(pattern)
    for ch in ("*", "?"):
        idx = pattern.find(ch)
        if idx != -1 and idx < first_wild:
            first_wild = idx
    return pattern[: pattern.rfind("/", 0, first_wild) + 1]


def ls_pattern(pattern: str) -> tuple[str, list[str]]:
    """Run s5cmd ls <pattern> and return (pattern, [full_s3_uri, ...])."""
    prefix = _prefix_for(pattern)
    result = subprocess.run(
        ["s5cmd", "ls", pattern],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "no object found" not in stderr:
            log.error("s5cmd ls failed for %s: %s", pattern, stderr)
        return pattern, []

    uris = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        rel_path = line.split()[-1]
        uris.append(prefix + rel_path)
    return pattern, uris


def main() -> None:
    args = parse_args()
    entries = read_patterns(args.csv_path)
    if not entries:
        log.error("No valid Npy paths found in %s", args.csv_path)
        sys.exit(1)

    # Collect all patterns to run
    all_patterns = []
    pattern_to_name: dict[str, str] = {}
    for entry in entries:
        for pat in entry["patterns"]:
            all_patterns.append(pat)
            pattern_to_name[pat] = entry["name"]

    log.info(
        "Listing %d patterns across %d datasets with %d workers...",
        len(all_patterns), len(entries), args.workers,
    )

    # Run all ls commands in parallel
    all_uris: list[str] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(ls_pattern, p): p for p in all_patterns}
        for fut in as_completed(futures):
            pattern, uris = fut.result()
            name = pattern_to_name[pattern]
            if uris:
                log.info("  %-40s  %6d files  (%s)", name, len(uris), pattern.split("/")[-1])
            all_uris.extend(uris)

    if not all_uris:
        log.error("No files found for any pattern.")
        sys.exit(1)

    # Write manifest
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        for uri in sorted(all_uris):
            parsed = urlparse(uri)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            writer.writerow([bucket, key])

    log.info("Wrote %d entries to %s", len(all_uris), args.output)


if __name__ == "__main__":
    main()
