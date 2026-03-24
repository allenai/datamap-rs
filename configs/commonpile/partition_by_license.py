# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "smart_open[all]",
#   "msgspec",
#   "tqdm",
# ]
# ///
"""Partition json.gz/jsonl.zst files by the `metadata.license` field, maintaining directory structure."""

import argparse
import os
import random
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import msgspec
import smart_open


_CC_COMPONENTS = ["BY", "NC", "ND", "SA"]


def normalize_license(license_value: str | None) -> str:
    if license_value is None:
        return "null"

    s = license_value.upper()

    # Parse CC licenses into components: CCBYNCSA -> cc_by_nc_sa
    if s.startswith("CC"):
        rest = s[2:]
        if rest == "0":
            return "cc0"
        parts = ["cc"]
        for component in _CC_COMPONENTS:
            if rest.startswith(component):
                parts.append(component.lower())
                rest = rest[len(component):]
        if not rest:
            return "_".join(parts)

    # Non-CC: lowercase, replace separators with underscore, strip the rest
    return re.sub(r"[^a-z0-9_]", "", license_value.lower().replace("-", "_").replace(" ", "_"))


def process_batch(input_paths: list[Path], output_dir: Path, batch_idx: int) -> int:
    """Process a batch of json.gz/jsonl.zst files, partitioning rows by license.

    Returns the number of rows processed.
    """
    encoder = msgspec.json.Encoder()
    decoder = msgspec.json.Decoder()

    # Collect rows by license across all files in the batch
    buckets: dict[str, list[bytes]] = {}

    for input_path in input_paths:
        with smart_open.open(str(input_path), "rb") as fin:
            for line in fin:
                row = decoder.decode(line)
                metadata = row.get("metadata")
                license_value = metadata.get("license") if isinstance(metadata, dict) else None
                key = normalize_license(license_value)
                if key not in buckets:
                    buckets[key] = []
                buckets[key].append(encoder.encode(row))

    output_dir.mkdir(parents=True, exist_ok=True)
    for license_key, rows in buckets.items():
        out_path = output_dir / f"shard_{batch_idx:08d}_{license_key}.json.gz"
        with smart_open.open(str(out_path), "wb") as fout:
            for encoded_row in rows:
                fout.write(encoded_row + b"\n")

    return sum(len(rows) for rows in buckets.values())


def main():
    parser = argparse.ArgumentParser(description="Partition jsonl.zst files by metadata.license field.")
    parser.add_argument("--input-dir", type=Path, required=True, help="Directory with (nested) json.gz or jsonl.zst files.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for partitioned files.")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of parallel workers.")
    parser.add_argument("--estimated-licenses", type=int, default=8, help="Estimated number of distinct licenses; controls batch size.")
    args = parser.parse_args()

    input_dir: Path = args.input_dir.resolve()
    output_dir: Path = args.output_dir.resolve()

    # Walk input directory; for each subdirectory, shuffle json.gz/jsonl.zst files
    # then chunk into batches of --estimated-licenses.
    supported_extensions = (".json.gz", ".jsonl.zst")
    batch_size = max(1, args.estimated_licenses)
    tasks: list[tuple[list[Path], Path, int]] = []
    batch_idx = 0
    total_files = 0
    for dirpath, _, filenames in sorted(os.walk(input_dir)):
        files = [Path(dirpath) / f for f in filenames if any(f.endswith(ext) for ext in supported_extensions)]
        random.shuffle(files)
        if not files:
            continue
        total_files += len(files)
        rel_parent = Path(dirpath).relative_to(input_dir)
        out_subdir = output_dir / rel_parent
        for i in range(0, len(files), batch_size):
            tasks.append((files[i : i + batch_size], out_subdir, batch_idx))
            batch_idx += 1

    if not tasks:
        print(f"No json.gz or jsonl.zst files found in {input_dir}")
        return

    print(f"Found {total_files} input files, grouped into {len(tasks)} batches of ~{batch_size}")

    from tqdm import tqdm

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_batch, batch, out, idx): idx
            for batch, out, idx in tasks
        }
        total_rows = 0
        with tqdm(total=len(futures), desc="Processing batches", unit="batch") as pbar:
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    n = future.result()
                    total_rows += n
                except Exception as exc:
                    print(f"\nError processing batch {idx}: {exc}")
                pbar.update(1)

    print(f"Done. Processed {total_rows:,} rows across {total_files} files.")


if __name__ == "__main__":
    main()
