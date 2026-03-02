#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zstandard",
#     "tqdm",
#     "orjson",
# ]
# ///
"""Cross-source URL deduplication for PDF datasets.

Finds URLs that appear in 2+ sources and extracts full entries for inspection.

Sources:
  finepdfs -- HuggingFaceFW_finepdfs
  s2orc    -- s2orcforolmo
  crawled  -- olmo-crawled-pdfs

Output:
  url_dedupe_output/
    report.txt                  -- summary statistics
    duplicate_urls.jsonl        -- index: {url, in_sources} for every dup URL
    duplicates/<url_dirname>/   -- one folder per duplicate URL
      <source_name>.txt         -- the text field from that source for visual diff
"""

import argparse
import io
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import orjson
import zstandard as zstd
from tqdm import tqdm

SOURCES = {
    "s2orc": "/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/s2orcforolmo_reshard_urltagged_fostagged_norefs_partitioned",
    "crawled": "/mnt/raid0/ai2-llm/pretraining-data/sources/dolma4pdfs/olmo-crawled-pdfs_reshard_with_urls_wo_nospam_nopii_nobigtablesv5/step_final/",
    "finepdfs": "/mnt/raid0/ai2-llm/pretraining-data/sources/HuggingFaceFW_finepdfs/deduped_eng_nopii_qualitytagged_partitioned",
}

OUTPUT_DIR = Path("/mnt/raid0/url_dedupe_output")
NUM_WORKERS = os.cpu_count()


# ---------------------------------------------------------------------------
# Phase 1 helpers
# ---------------------------------------------------------------------------

def find_jsonl_zst_files(base_dir: str) -> list[str]:
    """Recursively find all .jsonl.zst files under a directory."""
    files = []
    for root, _, filenames in os.walk(base_dir):
        for fn in filenames:
            if fn.endswith(".jsonl.zst"):
                files.append(os.path.join(root, fn))
    return sorted(files)


def extract_urls(filepath: str) -> tuple[set[str], int]:
    """Read a .jsonl.zst file and return (set_of_urls, doc_count)."""
    urls: set[str] = set()
    n = 0
    dctx = zstd.ZstdDecompressor()
    try:
        with open(filepath, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                for line in io.BufferedReader(reader):
                    if not line.strip():
                        continue
                    n += 1
                    try:
                        doc = orjson.loads(line)
                        url = doc.get("url")
                        if url:
                            urls.add(url)
                    except Exception:
                        continue
    except Exception as e:
        print(f"\nError reading {filepath}: {e}", file=sys.stderr)
    return urls, n


# ---------------------------------------------------------------------------
# Phase 3 helpers
# ---------------------------------------------------------------------------

_dup_url_set: set[str] = set()


def _init_extract_worker(dup_urls: set[str]) -> None:
    global _dup_url_set
    _dup_url_set = dup_urls


def extract_dup_entries(args: tuple[str, str]) -> list[tuple[str, str, str]]:
    """Re-read a file and return text fields for docs whose URL is in the dup set.

    Returns list of (url, source_name, text).
    """
    filepath, source_name = args
    results: list[tuple[str, str, str]] = []
    dctx = zstd.ZstdDecompressor()
    try:
        with open(filepath, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                for line in io.BufferedReader(reader):
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        doc = orjson.loads(stripped)
                        url = doc.get("url")
                        if url and url in _dup_url_set:
                            text = doc.get("text", "")
                            results.append((url, source_name, text))
                    except Exception:
                        continue
    except Exception as e:
        print(f"\nError reading {filepath}: {e}", file=sys.stderr)
    return results


def url_to_dirname(url: str) -> str:
    """Convert a URL to a filesystem-safe directory name."""
    # Strip protocol
    name = re.sub(r"^https?://", "", url)
    # Replace any non-alphanumeric/dot/hyphen chars with underscores
    name = re.sub(r"[^a-zA-Z0-9.\-]", "_", name)
    # Collapse repeated underscores
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    # Truncate to stay within filesystem limits (keep room for parent path)
    if len(name) > 200:
        name = name[:200]
    return name


# ---------------------------------------------------------------------------
# Dedup writing helpers
# ---------------------------------------------------------------------------

_skip_urls: set[str] = set()


def _init_dedup_worker(skip: set[str]) -> None:
    global _skip_urls
    _skip_urls = skip


def write_deduped_file(args: tuple[str, str, str]) -> tuple[int, int]:
    """Re-read a source file, skip URLs in the skip set, write to output.

    Returns (total_docs, kept_docs).
    """
    filepath, source_base, out_base = args
    rel = os.path.relpath(filepath, source_base)
    out_path = os.path.join(out_base, rel)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    total = 0
    kept = 0
    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor()

    try:
        with open(filepath, "rb") as fh_in, open(out_path, "wb") as fh_out:
            with cctx.stream_writer(fh_out) as writer:
                with dctx.stream_reader(fh_in) as reader:
                    for line in io.BufferedReader(reader):
                        if not line.strip():
                            continue
                        total += 1
                        try:
                            doc = orjson.loads(line)
                            url = doc.get("url")
                            if url and url in _skip_urls:
                                continue
                        except Exception:
                            pass
                        kept += 1
                        writer.write(line)
    except Exception as e:
        print(f"\nError processing {filepath}: {e}", file=sys.stderr)

    return total, kept


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def write_report(
    source_doc_counts: dict[str, int],
    source_url_counts: dict[str, int],
    source_file_counts: dict[str, int],
    pair_overlaps: dict[tuple[str, str], int],
    num_dup_urls: int,
) -> None:
    lines = [
        "Cross-Source URL Deduplication Report",
        "=" * 60,
        "",
        "Per-source statistics:",
    ]
    for name in source_doc_counts:
        lines.append(f"  {name}:")
        lines.append(f"    Files:       {source_file_counts[name]:>12,}")
        lines.append(f"    Documents:   {source_doc_counts[name]:>12,}")
        lines.append(f"    Unique URLs: {source_url_counts[name]:>12,}")
    lines.append("")
    lines.append("Pairwise URL overlaps:")
    for (s1, s2), count in pair_overlaps.items():
        lines.append(f"  {s1} ∩ {s2}: {count:,}")
    lines.append("")
    lines.append(f"Total unique cross-source duplicate URLs: {num_dup_urls:,}")
    lines.append("")

    report = "\n".join(lines)
    report_path = OUTPUT_DIR / "report.txt"
    report_path.write_text(report)
    print(f"\n  Report → {report_path}")
    print()
    print(report)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cross-source URL deduplication for PDF datasets."
    )
    parser.add_argument(
        "--debug-n-files",
        type=int,
        default=None,
        metavar="N",
        help="Debug mode: only read the first N files per source",
    )
    parser.add_argument(
        "--skip-text-output",
        action="store_true",
        help="Skip writing per-URL duplicate text folders (still writes report and index)",
    )
    parser.add_argument(
        "--write-deduped",
        type=str,
        default=None,
        metavar="DIR",
        help="Write deduplicated datasets to DIR, resolving cross-source duplicates by source priority order",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.debug_n_files is not None:
        print(f"*** DEBUG MODE: reading at most {args.debug_n_files} files per source ***\n")

    # ── Phase 1: Collect URL sets per source ──────────────────────
    print("=" * 60)
    print("Phase 1: Extracting URLs from all sources")
    print("=" * 60)

    source_url_sets: dict[str, set[str]] = {}
    source_file_lists: dict[str, list[str]] = {}
    source_doc_counts: dict[str, int] = {}
    source_url_counts: dict[str, int] = {}
    source_file_counts: dict[str, int] = {}

    for source_name, base_dir in SOURCES.items():
        print(f"\nSource: {source_name}")
        files = find_jsonl_zst_files(base_dir)
        if args.debug_n_files is not None:
            files = files[: args.debug_n_files]
        source_file_lists[source_name] = files
        source_file_counts[source_name] = len(files)
        print(f"  Files: {len(files):,}")

        all_urls: set[str] = set()
        total_docs = 0

        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futures = {pool.submit(extract_urls, f): f for f in files}
            for fut in tqdm(
                as_completed(futures), total=len(futures), desc=f"  {source_name}"
            ):
                urls, count = fut.result()
                all_urls.update(urls)
                total_docs += count

        source_url_sets[source_name] = all_urls
        source_doc_counts[source_name] = total_docs
        source_url_counts[source_name] = len(all_urls)
        print(f"  Documents: {total_docs:,}")
        print(f"  Unique URLs: {len(all_urls):,}")

    # ── Phase 2: Find cross-source duplicates ─────────────────────
    print("\n" + "=" * 60)
    print("Phase 2: Finding cross-source duplicates")
    print("=" * 60)

    source_names = list(source_url_sets.keys())
    pair_overlaps: dict[tuple[str, str], int] = {}
    dup_urls: set[str] = set()

    for i in range(len(source_names)):
        for j in range(i + 1, len(source_names)):
            s1, s2 = source_names[i], source_names[j]
            overlap = source_url_sets[s1] & source_url_sets[s2]
            pair_overlaps[(s1, s2)] = len(overlap)
            dup_urls.update(overlap)
            print(f"  {s1} ∩ {s2}: {len(overlap):,} shared URLs")

    print(f"\n  Total cross-source duplicate URLs: {len(dup_urls):,}")

    # Compute per-source skip sets for dedup writing (before freeing URL sets)
    source_priority = list(SOURCES.keys())
    urls_to_remove: dict[str, set[str]] = {name: set() for name in source_priority}
    if args.write_deduped and dup_urls:
        for url in dup_urls:
            sources_with = [s for s in source_priority if url in source_url_sets[s]]
            # Highest-priority source keeps it; all others lose it
            for s in sources_with[1:]:
                urls_to_remove[s].add(url)

    # Free the big sets
    del source_url_sets

    if not dup_urls:
        print("\nNo cross-source duplicates found.")
        if not args.write_deduped:
            write_report(
                source_doc_counts, source_url_counts, source_file_counts,
                pair_overlaps, 0,
            )
            return

    if args.skip_text_output:
        print("\n  Skipping phase 3 (--skip-text-output)")
        url_sources = {url: set() for url in dup_urls}
    else:
        # ── Phase 3: Extract full entries for duplicate URLs ──────────
        print("\n" + "=" * 60)
        print("Phase 3: Extracting full entries for duplicate URLs")
        print("=" * 60)

        all_tasks: list[tuple[str, str]] = []
        for source_name, files in source_file_lists.items():
            for f in files:
                all_tasks.append((f, source_name))

        # Collect: url -> {source_name: text}
        # (if a URL appears multiple times in one source, keep the first)
        url_texts: dict[str, dict[str, str]] = defaultdict(dict)
        url_sources: dict[str, set[str]] = defaultdict(set)
        entry_counts: dict[str, int] = defaultdict(int)

        with ProcessPoolExecutor(
            max_workers=NUM_WORKERS,
            initializer=_init_extract_worker,
            initargs=(dup_urls,),
        ) as pool:
            futures = {pool.submit(extract_dup_entries, t): t for t in all_tasks}
            for fut in tqdm(
                as_completed(futures), total=len(futures), desc="  Extracting"
            ):
                for url, source_name, text in fut.result():
                    url_sources[url].add(source_name)
                    entry_counts[source_name] += 1
                    if source_name not in url_texts[url]:
                        url_texts[url][source_name] = text

        # Write per-URL folders with source_name.txt files
        dup_dir = OUTPUT_DIR / "duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)

        for url, source_texts in url_texts.items():
            folder = dup_dir / url_to_dirname(url)
            folder.mkdir(parents=True, exist_ok=True)
            for source_name, text in source_texts.items():
                (folder / f"{source_name}.txt").write_text(text, encoding="utf-8")

        print(f"  {len(url_texts):,} URL folders → {dup_dir}/")
        for source_name, count in entry_counts.items():
            print(f"  {source_name}: {count:,} entries")

    # ── Write deduplicated datasets ──────────────────────────────
    if args.write_deduped:
        print("\n" + "=" * 60)
        print("Writing deduplicated datasets")
        print("=" * 60)

        write_base = Path(args.write_deduped)

        for source_name in source_priority:
            skip = urls_to_remove[source_name]
            source_base = SOURCES[source_name]
            out_base = str(write_base / source_name)
            files = source_file_lists[source_name]

            print(f"\n  {source_name}: {len(files):,} files, removing {len(skip):,} duplicate URLs")

            tasks = [(f, source_base, out_base) for f in files]

            total_docs = 0
            kept_docs = 0

            with ProcessPoolExecutor(
                max_workers=NUM_WORKERS,
                initializer=_init_dedup_worker,
                initargs=(skip,),
            ) as pool:
                futures = {pool.submit(write_deduped_file, t): t for t in tasks}
                for fut in tqdm(
                    as_completed(futures), total=len(futures), desc=f"  {source_name}"
                ):
                    t, k = fut.result()
                    total_docs += t
                    kept_docs += k

            print(f"    {total_docs:,} docs -> {kept_docs:,} kept ({total_docs - kept_docs:,} removed)")

    # ── Phase 4: Write URL index & report ─────────────────────────
    print("\n" + "=" * 60)
    print("Phase 4: Writing index and report")
    print("=" * 60)

    index_path = OUTPUT_DIR / "duplicate_urls.jsonl"
    with open(index_path, "wb") as f:
        for url in sorted(url_sources):
            record = {"url": url, "in_sources": sorted(url_sources[url])}
            f.write(orjson.dumps(record) + b"\n")
    print(f"  {len(url_sources):,} duplicate URLs → {index_path}")

    write_report(
        source_doc_counts, source_url_counts, source_file_counts,
        pair_overlaps, len(url_sources),
    )


if __name__ == "__main__":
    main()
