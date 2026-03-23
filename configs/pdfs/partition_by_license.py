# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "smart_open[zst]",
#   "msgspec",
#   "tqdm",
# ]
# ///
"""Partition jsonl.zst files by the `license` field, maintaining directory structure."""

import argparse
import os
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


def process_file(input_path: Path, output_dir: Path) -> int:
    """Process a single jsonl.zst file, partitioning rows by license.

    Returns the number of rows processed.
    """
    encoder = msgspec.json.Encoder()
    decoder = msgspec.json.Decoder()

    # Collect rows by license
    buckets: dict[str, list[bytes]] = {}

    with smart_open.open(str(input_path), "rb") as fin:
        for line in fin:
            row = decoder.decode(line)
            license_value = row.get("license")
            key = normalize_license(license_value)
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(encoder.encode(row))

    # Write each license bucket to its own file
    stem = input_path.stem
    if stem.endswith(".jsonl"):
        stem = stem[: -len(".jsonl")]

    for license_key, rows in buckets.items():
        out_path = output_dir / f"{stem}_{license_key}.jsonl.zst"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with smart_open.open(str(out_path), "wb") as fout:
            for encoded_row in rows:
                fout.write(encoded_row + b"\n")

    return sum(len(rows) for rows in buckets.values())


def main():
    parser = argparse.ArgumentParser(description="Partition jsonl.zst files by license field.")
    parser.add_argument("--input-dir", type=Path, required=True, help="Directory with (nested) jsonl.zst files.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for partitioned files.")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of parallel workers.")
    args = parser.parse_args()

    input_dir: Path = args.input_dir.resolve()
    output_dir: Path = args.output_dir.resolve()

    # Discover all jsonl.zst files
    input_paths = sorted(input_dir.rglob("*.jsonl.zst"))
    if not input_paths:
        print(f"No .jsonl.zst files found in {input_dir}")
        return

    # Build (input_path, output_subdir) pairs preserving directory structure
    tasks: list[tuple[Path, Path]] = []
    for p in input_paths:
        rel = p.relative_to(input_dir)
        out_subdir = output_dir / rel.parent
        tasks.append((p, out_subdir))

    from tqdm import tqdm

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_file, inp, out): inp for inp, out in tasks}
        total_rows = 0
        with tqdm(total=len(futures), desc="Processing files", unit="file") as pbar:
            for future in as_completed(futures):
                inp = futures[future]
                try:
                    n = future.result()
                    total_rows += n
                except Exception as exc:
                    print(f"\nError processing {inp}: {exc}")
                pbar.update(1)

    print(f"Done. Processed {total_rows:,} rows across {len(tasks)} files.")


if __name__ == "__main__":
    main()
