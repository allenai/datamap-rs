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


def glob_to_regex(pattern: str) -> re.Pattern:
    """Convert a glob pattern to a regex where * matches any non-/ character sequence."""
    parts = pattern.split("*")
    escaped = [re.escape(p) for p in parts]
    return re.compile("^" + "[^/]*".join(escaped) + "$")


def list_common_prefixes(bucket: str, prefix: str) -> list[str]:
    """List immediate subdirectories (common prefixes) under the given prefix."""
    s3 = boto3.client("s3")
    prefixes = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.append(cp["Prefix"])

    return prefixes


def calc_size_for_prefix(bucket: str, prefix: str, glob_regex: re.Pattern | None, pattern: str | None) -> tuple[int, int]:
    """Calculate total size and count of matching files under a prefix (recursive).

    In glob mode (glob_regex is set), matches files against the glob pattern.
    Otherwise, matches only .npy files. An additional regex filter can be applied via pattern.
    """
    s3 = boto3.client("s3")
    total_size = 0
    total_count = 0
    paginator = s3.get_paginator("list_objects_v2")
    regex = re.compile(pattern) if pattern else None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if glob_regex is not None:
                if not glob_regex.match(key):
                    continue
            else:
                if not key.endswith(".npy"):
                    continue
            if regex is not None and not regex.search(key):
                continue
            total_size += obj["Size"]
            total_count += 1

    return total_size, total_count


def format_size(size_bytes: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} EB"


def main():
    parser = argparse.ArgumentParser(
        description="Calculate total size of files under an S3 prefix, optionally filtered by a glob or regex pattern"
    )
    parser.add_argument("s3_prefix", help="S3 prefix or glob pattern (e.g., s3://bucket/path/*.gz)")
    parser.add_argument(
        "-p", "--pattern", type=str, default=None,
        help="Regex pattern to additionally filter paths (e.g., '/quality_p\\d+/')"
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
    bucket, _, rest = path.partition("/")

    glob_regex = None
    if "*" in rest:
        # Glob mode: find longest prefix before any path component containing *
        glob_pattern = rest
        parts = rest.split("/")
        stable_parts = []
        for part in parts:
            if "*" in part:
                break
            stable_parts.append(part)
        prefix = "/".join(stable_parts) + "/" if stable_parts else ""
        glob_regex = glob_to_regex(glob_pattern)
        print(f"Scanning s3://{bucket}/{prefix} with glob: {glob_pattern}")
    else:
        prefix = rest
        if not prefix.endswith("/"):
            prefix += "/"
        print(f"Scanning s3://{bucket}/{prefix}")

    print(f"Using {args.workers} workers")
    if args.pattern:
        print(f"Additional regex filter: {args.pattern}")
    if glob_regex is None:
        print("Counting all *.npy files")

    # Get top-level prefixes to distribute work
    top_prefixes = list_common_prefixes(bucket, prefix)

    if not top_prefixes:
        # No subdirectories, scan the prefix directly
        top_prefixes = [prefix]

    print(f"Found {len(top_prefixes)} top-level prefixes to scan")

    total_size = 0
    total_count = 0
    calc_fn = partial(calc_size_for_prefix, bucket, glob_regex=glob_regex, pattern=args.pattern)

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(calc_fn, p): p for p in top_prefixes}

        for future in as_completed(futures):
            prefix_path = futures[future]
            try:
                size, count = future.result()
                if size > 0:
                    print(f"  {prefix_path}: {format_size(size)} ({count:,} files)")
                total_size += size
                total_count += count
            except Exception as e:
                print(f"  Error processing {prefix_path}: {e}")

    print(f"\nTotal size: {format_size(total_size)} ({total_size:d} bytes, {total_count:,} files)")


if __name__ == "__main__":
    main()
