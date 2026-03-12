#!/usr/bin/env python3
"""
Copy .npy files listed in a CSV's "Npy paths" column to a destination S3 bucket,
preserving the path structure after the source bucket name.

Usage:
    python move_to_bucket.py <csv_path> [--dest-bucket BUCKET] [--dry-run] [--workers N]

Requires s5cmd to be installed and on $PATH.
"""

import argparse
import csv
import logging
import subprocess
import sys
import tempfile
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DEST_BUCKET = "<FILLME>"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv_path", help="Path to the input CSV file")
    p.add_argument(
        "--dest-bucket",
        default=DEST_BUCKET,
        help=f"Destination S3 bucket (default: {DEST_BUCKET})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the s5cmd commands that would be run; don't execute.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=256,
        help="Number of parallel workers for s5cmd (default: 256)",
    )
    return p.parse_args()


def read_npy_paths(csv_path: str) -> list[dict]:
    """Return a list of {name, src_pattern, dest_prefix} dicts from the CSV."""
    entries = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Name", "").strip()
            npy_path = row.get("Npy paths", "").strip()
            if not npy_path:
                continue

            parsed = urlparse(npy_path)
            src_bucket = parsed.netloc  # e.g. "ai2-llm"
            key_pattern = parsed.path.lstrip("/")  # everything after bucket

            # Strip the glob portion to get the "directory" prefix for the dest
            # e.g. preprocessed/foo/bar/*/allenai/dolma2-tokenizer/*.npy
            # We keep the full key structure so the dest mirrors the source.
            entries.append(
                {
                    "name": name,
                    "src_pattern": npy_path,
                    "src_bucket": src_bucket,
                    "key_pattern": key_pattern,
                }
            )
    return entries


def run_copy(
    entries: list[dict],
    dest_bucket: str,
    dry_run: bool,
    workers: int,
) -> None:
    """
    For each entry:
      1. `s5cmd ls <glob>` to enumerate concrete S3 keys.
      2. Build a command file mapping each source key to the destination key
         (same path, different bucket).
      3. `s5cmd run <command_file>` to execute all copies in parallel.
    """
    for entry in entries:
        src_pattern = entry["src_pattern"]
        src_bucket = entry["src_bucket"]
        name = entry["name"]
        log.info("Processing dataset %r  —  pattern: %s", name, src_pattern)

        # Step 1: list matching files
        ls_cmd = ["s5cmd", "ls", src_pattern]
        log.info("  Listing files: %s", " ".join(ls_cmd))

        if dry_run:
            log.info("  [dry-run] would run: %s", " ".join(ls_cmd))
            log.info(
                "  [dry-run] then copy each listed file to s3://%s/<same-key>",
                dest_bucket,
            )
            continue

        result = subprocess.run(ls_cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            log.error(
                "  s5cmd ls failed (rc=%d): %s",
                result.returncode,
                result.stderr.strip(),
            )
            continue

        # Parse ls output.  Each line looks like:
        #   2024/01/15 12:34:56       12345  s3://bucket/key
        lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
        if not lines:
            log.warning("  No files matched pattern — skipping.")
            continue

        log.info("  Found %d files.", len(lines))

        # Step 2: build a command file for `s5cmd run`
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="s5cmd_run_"
        ) as cmd_file:
            for line in lines:
                # Extract the s3:// URI from the ls output
                parts = line.split()
                # The URI is the last token
                src_uri = parts[-1]
                if not src_uri.startswith("s3://"):
                    log.warning("  Skipping unparseable line: %s", line)
                    continue

                # Rewrite bucket: s3://ai2-llm/key -> s3://<dest_bucket>/key
                parsed = urlparse(src_uri)
                dest_uri = f"s3://{dest_bucket}/{parsed.path.lstrip('/')}"

                cmd_file.write(f"cp {src_uri} {dest_uri}\n")

            cmd_file_path = cmd_file.name

        # Step 3: execute with s5cmd run
        run_cmd = [
            "s5cmd",
            "--numworkers",
            str(workers),
            "run",
            cmd_file_path,
        ]
        log.info("  Running: %s", " ".join(run_cmd))

        proc = subprocess.run(run_cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            log.error(
                "  s5cmd run failed (rc=%d):\n%s",
                proc.returncode,
                proc.stderr.strip(),
            )
        else:
            log.info("  Done — copied %d files for %r.", len(lines), name)

        # Cleanup temp file
        try:
            import os

            os.unlink(cmd_file_path)
        except Exception:
            pass


def main() -> None:
    args = parse_args()

    entries = read_npy_paths(args.csv_path)
    if not entries:
        log.error("No non-empty 'Npy paths' found in %s", args.csv_path)
        sys.exit(1)

    log.info(
        "Found %d datasets with Npy paths to copy -> s3://%s",
        len(entries),
        args.dest_bucket,
    )

    run_copy(entries, args.dest_bucket, args.dry_run, args.workers)
    log.info("All done.")


if __name__ == "__main__":
    main()
