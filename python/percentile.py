#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#     "jq",
#     "msgspec",
#     "smart-open[zst,gz]",
#     "tqdm",
#     "numpy",
#     "click",
# ]
# ///
"""
Calculate percentiles from values extracted from zstd JSONL files.

Samples N lines proportionally to file size across all files, extracts values
using a jq expression, and computes percentiles. Supports weighted percentiles
where weights can be text length (so p20 = threshold under which ~20% of chars fall).

Usage:
    uv run scripts/percentiles.py /path/to/data -e '.score' -n 1000000
    uv run scripts/percentiles.py /path/to/data -e '.score' --weight-by '.text | length'
    uv run scripts/percentiles.py /path/to/data -e '.attributes.quality' --percentiles 10 25 50 75 90 99
"""

from __future__ import annotations

import os
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

import click
import jq
import msgspec
import numpy as np
import smart_open
from tqdm import tqdm

FILE_SUFFIXES = frozenset(
    f"{type_}{compr}"
    for type_ in (".jsonl", ".json")
    for compr in (".zst", ".zstd", ".gz", ".gzip", "")
)


def compile_jq(jq_expr: str) -> Callable[[dict], Any]:
    """Compile a jq expression into a callable."""
    if not jq_expr.strip():
        return lambda x: x

    compiled_jq = jq.compile(jq_expr)

    def transform(x: dict, _compiled_jq=compiled_jq) -> Any:
        return _compiled_jq.input_value(x).first()

    return transform


def find_jsonl_files(directory: Path, recursive: bool = True) -> list[Path]:
    """Find all JSONL files in directory."""
    files: list[Path] = []
    if recursive:
        for root, _, filenames in os.walk(directory):
            for filename in filenames:
                file_path = Path(root) / filename
                if "".join(file_path.suffixes) in FILE_SUFFIXES:
                    files.append(file_path)
    else:
        for file_path in directory.iterdir():
            if file_path.is_file() and "".join(file_path.suffixes) in FILE_SUFFIXES:
                files.append(file_path)
    return sorted(files)


def get_file_sizes(files: list[Path]) -> list[int]:
    """Get compressed file sizes."""
    return [f.stat().st_size for f in files]


def sample_values_from_file(
    file_path: Path,
    value_expression: str,
    weight_expression: str | None,
    target_samples: int,
    seed: int,
) -> list[tuple[float, float]]:
    """Sample (value, weight) pairs from a single file.

    Returns list of (value, weight) tuples. If weight_expression is None, weight=1.0.
    """
    rng = random.Random(seed)
    decoder = msgspec.json.Decoder()
    extract_value = compile_jq(value_expression)
    extract_weight = compile_jq(weight_expression) if weight_expression else None

    samples: list[tuple[float, float]] = []

    # First pass: count lines to determine sampling rate
    with smart_open.open(file_path, "rb") as f:
        line_count = sum(1 for _ in f)

    if line_count == 0:
        return []

    # If we need all or most lines, just read them all
    if target_samples >= line_count:
        with smart_open.open(file_path, "rb") as f:
            for line in f:
                row = decoder.decode(line)
                value = extract_value(row)
                if value is None:
                    continue
                try:
                    v = float(value)
                except (TypeError, ValueError):
                    continue

                if extract_weight:
                    w = extract_weight(row)
                    try:
                        w = float(w)
                    except (TypeError, ValueError):
                        w = 1.0
                else:
                    w = 1.0

                samples.append((v, w))
        return samples

    # Use probabilistic sampling for memory efficiency
    sample_rate = min(1.0, (target_samples * 1.2) / line_count)

    with smart_open.open(file_path, "rb") as f:
        for line in f:
            if rng.random() < sample_rate:
                row = decoder.decode(line)
                value = extract_value(row)
                if value is None:
                    continue
                try:
                    v = float(value)
                except (TypeError, ValueError):
                    continue

                if extract_weight:
                    w = extract_weight(row)
                    try:
                        w = float(w)
                    except (TypeError, ValueError):
                        w = 1.0
                else:
                    w = 1.0

                samples.append((v, w))

    # If we got more than needed, subsample
    if len(samples) > target_samples:
        samples = rng.sample(samples, target_samples)

    return samples


def weighted_percentile(
    values: np.ndarray, weights: np.ndarray, percentiles: list[float]
) -> list[float]:
    """Calculate weighted percentiles.

    For percentile p, finds the value v such that sum(weights where value < v) / sum(weights) ≈ p/100.
    """
    # Sort by value
    sort_idx = np.argsort(values)
    sorted_values = values[sort_idx]
    sorted_weights = weights[sort_idx]

    # Cumulative weights (use midpoint for each sample's contribution)
    cumsum = np.cumsum(sorted_weights)
    total = cumsum[-1]

    # Normalize to 0-100 scale, using midpoint of each sample's weight range
    # This means a sample contributes half its weight before and half after
    cumsum_mid = cumsum - sorted_weights / 2
    pct_scale = cumsum_mid / total * 100

    results = []
    for p in percentiles:
        # Find where p falls in the cumulative distribution
        idx = np.searchsorted(pct_scale, p)
        if idx == 0:
            results.append(float(sorted_values[0]))
        elif idx >= len(sorted_values):
            results.append(float(sorted_values[-1]))
        else:
            # Linear interpolation between adjacent values
            p_low = pct_scale[idx - 1]
            p_high = pct_scale[idx]
            v_low = sorted_values[idx - 1]
            v_high = sorted_values[idx]
            if p_high == p_low:
                results.append(float(v_low))
            else:
                frac = (p - p_low) / (p_high - p_low)
                results.append(float(v_low + frac * (v_high - v_low)))

    return results


def calculate_percentiles(
    values: np.ndarray,
    weights: np.ndarray,
    percentiles: list[float],
) -> dict[str, float]:
    """Calculate percentiles and basic statistics."""
    total_weight = np.sum(weights)
    weighted_mean = np.sum(values * weights) / total_weight
    weighted_var = np.sum(weights * (values - weighted_mean) ** 2) / total_weight

    results: dict[str, float] = {
        "count": len(values),
        "total_weight": float(total_weight),
        "mean": float(weighted_mean),
        "std": float(np.sqrt(weighted_var)),
        "min": float(np.min(values)),
        "max": float(np.max(values)),
    }

    pct_values = weighted_percentile(values, weights, percentiles)
    for p, v in zip(percentiles, pct_values):
        results[f"p{p:g}"] = v

    return results


def calculate_unweighted_percentiles(
    values: np.ndarray,
    percentiles: list[float],
) -> dict[str, float]:
    """Calculate unweighted percentiles and basic statistics."""
    results: dict[str, float] = {
        "count": len(values),
        "mean": float(np.mean(values)),
        "std": float(np.std(values)),
        "min": float(np.min(values)),
        "max": float(np.max(values)),
    }

    for p in percentiles:
        results[f"p{p:g}"] = float(np.percentile(values, p))

    return results


@click.command()
@click.argument(
    "directory", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "-e",
    "--expression",
    type=str,
    required=True,
    help="JQ expression to extract numeric value (e.g., '.score', '.attributes.quality')",
)
@click.option(
    "--weight-by",
    type=str,
    default=None,
    help="JQ expression for weight (e.g., '.text | length'). "
    "When set, percentiles are weighted so p20 = threshold under which ~20%% of weight falls.",
)
@click.option(
    "-n",
    "--num-samples",
    type=int,
    default=1_000_000,
    show_default=True,
    help="Total number of samples to collect across all files",
)
@click.option(
    "-p",
    "--percentiles",
    type=float,
    multiple=True,
    default=(1, 5, 10, 25, 50, 75, 90, 95, 99),
    show_default=True,
    help="Percentiles to calculate (can specify multiple times)",
)
@click.option(
    "-w",
    "--workers",
    type=int,
    default=None,
    help="Number of parallel workers (default: CPU count)",
)
@click.option(
    "-s",
    "--seed",
    type=int,
    default=42,
    show_default=True,
    help="Random seed for reproducibility",
)
@click.option(
    "--no-recursive",
    is_flag=True,
    help="Don't search subdirectories",
)
def main(
    directory: Path,
    expression: str,
    weight_by: str | None,
    num_samples: int,
    percentiles: tuple[float, ...],
    workers: int | None,
    seed: int,
    no_recursive: bool,
):
    """Calculate percentiles from values in JSONL files.

    Samples N lines proportionally to file size, extracts values using jq,
    and computes percentiles. Optionally weights by text length.

    Examples:

        uv run scripts/percentiles.py /data -e '.score'

        uv run scripts/percentiles.py /data -e '.score' --weight-by '.text | length'

        uv run scripts/percentiles.py /data -e '.attributes.quality' -n 500000

        uv run scripts/percentiles.py /data -e '.label' -p 25 -p 50 -p 75
    """
    workers = workers or os.cpu_count() or 1

    # Find all files
    files = find_jsonl_files(directory, recursive=not no_recursive)
    if not files:
        click.echo(f"No JSONL files found in {directory}", err=True)
        raise SystemExit(1)

    click.echo(f"Found {len(files)} files")

    # Get file sizes and calculate proportional samples per file
    sizes = get_file_sizes(files)
    total_size = sum(sizes)

    samples_per_file: list[int] = []
    for size in sizes:
        proportion = size / total_size
        samples_per_file.append(max(1, int(num_samples * proportion)))

    # Adjust to hit target exactly
    diff = num_samples - sum(samples_per_file)
    if diff > 0:
        sorted_indices = sorted(range(len(sizes)), key=lambda i: sizes[i], reverse=True)
        for i in range(min(diff, len(sorted_indices))):
            samples_per_file[sorted_indices[i]] += 1

    click.echo(f"Sampling ~{sum(samples_per_file):,} values across {len(files)} files")
    if weight_by:
        click.echo(f"Weighting by: {weight_by}")

    # Sample in parallel
    all_samples: list[tuple[float, float]] = []
    rng = random.Random(seed)

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for file_path, target in zip(files, samples_per_file):
            file_seed = rng.randint(0, 2**31)
            future = pool.submit(
                sample_values_from_file,
                file_path,
                expression,
                weight_by,
                target,
                file_seed,
            )
            futures[future] = file_path

        with tqdm(total=len(futures), desc="Sampling files", unit=" files") as pbar:
            for future in as_completed(futures):
                try:
                    samples = future.result()
                    all_samples.extend(samples)
                except Exception as e:
                    file_path = futures[future]
                    click.echo(f"Error processing {file_path}: {e}", err=True)
                pbar.update(1)

    if not all_samples:
        click.echo("No values extracted. Check your jq expression.", err=True)
        raise SystemExit(1)

    # Convert to arrays
    values = np.array([s[0] for s in all_samples])
    weights = np.array([s[1] for s in all_samples])

    click.echo(f"\nCollected {len(values):,} samples")

    # Calculate percentiles
    if weight_by:
        results = calculate_percentiles(values, weights, list(percentiles))
    else:
        results = calculate_unweighted_percentiles(values, list(percentiles))

    # Print results
    click.echo("\n" + "=" * 50)
    click.echo("STATISTICS")
    click.echo("=" * 50)
    click.echo(f"  Count: {results['count']:,}")
    if weight_by and "total_weight" in results:
        click.echo(f"  Total weight: {results['total_weight']:,.0f}")
    click.echo(f"  Mean:  {results['mean']:.6f}")
    click.echo(f"  Std:   {results['std']:.6f}")
    click.echo(f"  Min:   {results['min']:.6f}")
    click.echo(f"  Max:   {results['max']:.6f}")

    click.echo("\n" + "-" * 50)
    if weight_by:
        click.echo("PERCENTILES (weighted)")
    else:
        click.echo("PERCENTILES")
    click.echo("-" * 50)
    for p in sorted(percentiles):
        key = f"p{p:g}"
        click.echo(f"  {key:>6}: {results[key]:.6f}")


if __name__ == "__main__":
    main()
