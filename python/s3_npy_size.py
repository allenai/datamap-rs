# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "boto3",
# ]
# ///
"""Calculate total size of .npy files under an S3 prefix, optionally filtered by a regex pattern."""

import argparse
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial

import boto3


def list_common_prefixes(bucket: str, prefix: str) -> list[str]:
    """List immediate subdirectories (common prefixes) under the given prefix."""
    s3 = boto3.client("s3")
    prefixes = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])

    return prefixes


def calc_npy_size_for_prefix(bucket: str, prefix: str, pattern: str | None) -> int:
    """Calculate total size of .npy files under a prefix (recursive), optionally filtering by pattern."""
    s3 = boto3.client("s3")
    total_size = 0
    paginator = s3.get_paginator("list_objects_v2")
    regex = re.compile(pattern) if pattern else None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".npy"):
                if regex is None or regex.search(key):
                    total_size += obj["Size"]

    return total_size


def format_size(size_bytes: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} EB"


def main():
    parser = argparse.ArgumentParser(
        description="Calculate total size of .npy files under an S3 prefix, optionally filtered by a regex pattern"
    )
    parser.add_argument("s3_prefix", help="S3 prefix (e.g., s3://bucket/path/to/data/)")
    parser.add_argument(
        "-p", "--pattern", type=str, default=None,
        help="Regex pattern to filter paths (e.g., '/quality_p\\d+/'). If not provided, all .npy files are counted."
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=os.cpu_count(),
        help=f"Number of parallel workers (default: {os.cpu_count()})"
    )
    args = parser.parse_args()

    # Parse S3 URI
    if not args.s3_prefix.startswith("s3://"):
        raise ValueError("S3 prefix must start with s3://")

    path = args.s3_prefix[5:]
    bucket, _, prefix = path.partition("/")

    if not prefix.endswith("/"):
        prefix += "/"

    print(f"Scanning s3://{bucket}/{prefix}")
    print(f"Using {args.workers} workers")
    if args.pattern:
        print(f"Filtering for paths matching: {args.pattern} and *.npy")
    else:
        print("Counting all *.npy files")

    # Get top-level prefixes to distribute work
    top_prefixes = list_common_prefixes(bucket, prefix)

    if not top_prefixes:
        # No subdirectories, scan the prefix directly
        top_prefixes = [prefix]

    print(f"Found {len(top_prefixes)} top-level prefixes to scan")

    total_size = 0
    calc_fn = partial(calc_npy_size_for_prefix, bucket, pattern=args.pattern)

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(calc_fn, p): p for p in top_prefixes}

        for future in as_completed(futures):
            prefix_path = futures[future]
            try:
                size = future.result()
                if size > 0:
                    print(f"  {prefix_path}: {format_size(size)}")
                total_size += size
            except Exception as e:
                print(f"  Error processing {prefix_path}: {e}")

    print(f"\nTotal .npy size: {format_size(total_size)} ({total_size:,} bytes)")


if __name__ == "__main__":
    main()
