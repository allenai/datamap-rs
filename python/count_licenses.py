#!/usr/bin/env python3
"""Count occurrences of each .license value across all jsonl.zst files in s2orcforolmo."""

import json
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

INPUT_DIR = (
    "/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/"
    "dolma4pdfs_full_deduped_partitioned_resharded_qualitytagged_partitioned_decon/"
    "s2orcforolmo"
)


def count_file(filepath: str) -> Counter:
    import io

    import zstandard as zstd

    counts = Counter()
    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text_stream:
                doc = json.loads(line)
                counts[doc.get("license")] += 1
    return counts


def main():
    files = sorted(str(p) for p in Path(INPUT_DIR).rglob("*.jsonl.zst"))
    print(f"Found {len(files)} files")

    total = Counter()
    workers = min(os.cpu_count() or 1, 80)

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(count_file, f): f for f in files}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            total += fut.result()

    print(f"\n{'License':<30} {'Count':>12}")
    print("-" * 43)
    for license_val, count in total.most_common():
        label = repr(license_val) if license_val is None else license_val
        print(f"{label:<30} {count:>12,}")
    print("-" * 43)
    print(f"{'TOTAL':<30} {sum(total.values()):>12,}")


if __name__ == "__main__":
    main()
