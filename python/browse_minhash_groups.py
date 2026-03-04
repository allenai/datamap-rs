#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "zstandard",
# ]
# ///
"""
Load all chunk_00000000.*.group.jsonl.zst files, filter to records with
metadata.minhash.cc_id, and write out individual .txt files grouped by cc_id
so you can browse duplicates easily.

Output structure:
  <output_dir>/
    <cc_id>/
      <idx>_<sanitized_url>.txt

Each .txt file contains the document text plus a header with metadata.
"""

import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict

import zstandard as zstd


def sanitize_filename(s, max_len=80):
    s = re.sub(r'https?://', '', s)
    s = re.sub(r'[^\w\-.]', '_', s)
    s = s.strip('_')
    if len(s) > max_len:
        s = s[:max_len]
    return s


def load_records(input_dir):
    pattern = os.path.join(input_dir, "chunk_00000000.*.group.jsonl.zst")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No files matching {pattern}")
        sys.exit(1)

    print(f"Found {len(files)} group files")

    groups = defaultdict(list)
    total = 0
    kept = 0

    for fpath in files:
        fname = os.path.basename(fpath)
        print(f"  Loading {fname} ...", end=" ", flush=True)
        file_total = 0
        file_kept = 0

        dctx = zstd.ZstdDecompressor()
        with open(fpath, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                buf = b""
                while True:
                    chunk = reader.read(1 << 20)  # 1MB
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if not line:
                            continue
                        file_total += 1
                        rec = json.loads(line)
                        mh = rec.get("metadata", {}).get("minhash")
                        if not mh or "cc_id" not in mh:
                            continue
                        file_kept += 1
                        groups[mh["cc_id"]].append(rec)

                # handle last line without trailing newline
                if buf.strip():
                    file_total += 1
                    rec = json.loads(buf)
                    mh = rec.get("metadata", {}).get("minhash")
                    if mh and "cc_id" in mh:
                        file_kept += 1
                        groups[mh["cc_id"]].append(rec)

        total += file_total
        kept += file_kept
        print(f"{file_total} records, {file_kept} with cc_id")

    print(f"\nTotal: {total} records, {kept} with cc_id, {len(groups)} unique cc_ids")
    return groups


def write_groups(groups, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # Only write groups that actually have duplicates (2+ members)
    dup_groups = {cc_id: recs for cc_id, recs in groups.items() if len(recs) >= 2}
    singleton_count = len(groups) - len(dup_groups)

    print(f"\nGroups with duplicates: {len(dup_groups)}")
    print(f"Singleton groups (skipped): {singleton_count}")
    print(f"Writing to {output_dir} ...")

    for cc_id, recs in sorted(dup_groups.items()):
        group_dir = os.path.join(output_dir, str(cc_id))
        os.makedirs(group_dir, exist_ok=True)

        # Sort by cc_idx within the group
        recs.sort(key=lambda r: r.get("metadata", {}).get("minhash", {}).get("cc_idx", 0))

        for rec in recs:
            mh = rec["metadata"]["minhash"]
            cc_idx = mh.get("cc_idx", 0)
            cc_size = mh.get("cc_size", 0)
            url = rec.get("url", "unknown")
            source = rec.get("source", "unknown")
            text = rec.get("text", "")
            priority = rec.get("metadata", {}).get("dedupe_priority", "?")

            safe_url = sanitize_filename(url)
            filename = f"{cc_idx:06d}_{safe_url}.txt"
            filepath = os.path.join(group_dir, filename)

            with open(filepath, "w") as f:
                f.write(f"cc_id:    {cc_id}\n")
                f.write(f"cc_idx:   {cc_idx} / {cc_size}\n")
                f.write(f"priority: {priority}\n")
                f.write(f"source:   {source}\n")
                f.write(f"url:      {url}\n")
                f.write(f"chars:    {len(text)}\n")
                f.write("=" * 80 + "\n")
                f.write(text)

    # Write a summary index file
    summary_path = os.path.join(output_dir, "_index.txt")
    with open(summary_path, "w") as f:
        f.write(f"Minhash Duplicate Groups Summary\n")
        f.write(f"================================\n\n")
        f.write(f"Total records with cc_id: {sum(len(r) for r in groups.values())}\n")
        f.write(f"Total unique cc_ids:      {len(groups)}\n")
        f.write(f"Groups with duplicates:   {len(dup_groups)}\n")
        f.write(f"Singleton groups:         {singleton_count}\n\n")

        f.write(f"{'cc_id':>20s}  {'size':>5s}  sample_url\n")
        f.write(f"{'-'*20}  {'-'*5}  {'-'*60}\n")
        for cc_id, recs in sorted(dup_groups.items(), key=lambda x: -len(x[1])):
            sample_url = recs[0].get("url", "?")[:60]
            f.write(f"{cc_id:>20d}  {len(recs):>5d}  {sample_url}\n")

    print(f"Written {sum(len(r) for r in dup_groups.values())} files across {len(dup_groups)} group folders")
    print(f"Summary index: {summary_path}")


def main():
    parser = argparse.ArgumentParser(description="Browse minhash duplicate groups as .txt files")
    parser.add_argument(
        "--input-dir",
        default="/mnt/raid0/url_and_mh_grouped_priority",
        help="Directory containing chunk_00000000.*.group.jsonl.zst files",
    )
    parser.add_argument(
        "--output-dir",
        default="/mnt/raid0/minhash_groups_browse",
        help="Output directory for grouped .txt files",
    )
    args = parser.parse_args()

    groups = load_records(args.input_dir)
    write_groups(groups, args.output_dir)


if __name__ == "__main__":
    main()
