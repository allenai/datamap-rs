#!/usr/bin/env python3
"""
Script to recursively count total words in .text field of JSONL files compressed with gzip or zstd.
Usage: python word_counter.py /path/to/files
"""

import argparse
import gzip
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

try:
    import zstandard as zstd
except ImportError:
    print("Warning: zstandard not installed. Install with: pip install zstandard")
    zstd = None

from tqdm import tqdm


def count_words_in_file(filepath: Path) -> Tuple[str, int, int]:
    """
    Process a single compressed file and count words in .text fields.
    
    Returns:
        Tuple of (filename, word_count, line_count)
    """
    word_count = 0
    line_count = 0
    
    try:
        # Open file based on extension
        if filepath.suffix == '.gz':
            open_func = gzip.open
            mode = 'rt'
        elif filepath.suffix in ['.zstd', '.zst']:
            if zstd is None:
                return (str(filepath), 0, 0)
            # For zstandard, we need to use a different approach
            def open_func(path, mode):
                return zstd.open(path, mode='rt')
            mode = 'rt'
        else:
            return (str(filepath), 0, 0)
        
        # Process the file
        with open_func(filepath, mode) as f:
            for line in f:
                line_count += 1
                try:
                    data = json.loads(line.strip())
                    # Get text field and count words
                    text = data.get('text', '')
                    if text:
                        # Simple word counting - split by whitespace
                        word_count += len(text.split())
                except json.JSONDecodeError:
                    # Skip invalid JSON lines
                    continue
                except Exception as e:
                    # Skip problematic lines
                    continue
                    
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return (str(filepath), 0, 0)
    
    return (str(filepath), word_count, line_count)


def find_compressed_files(directory: Path) -> List[Path]:
    """Find all .gz, .zstd, and .zst files recursively in the given directory."""
    extensions = ['.gz', '.zstd', '.zst']
    files = []
    
    for ext in extensions:
        files.extend(directory.rglob(f'*{ext}'))
    
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(
        description='Count words in .text fields of compressed JSONL files'
    )
    parser.add_argument(
        'path',
        type=str,
        help='Path to directory containing compressed files'
    )
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=None,
        help='Number of worker processes (default: number of CPU cores)'
    )
    
    args = parser.parse_args()
    
    # Validate path
    directory = Path(args.path)
    if not directory.exists():
        print(f"Error: Path '{directory}' does not exist")
        return 1
    
    if not directory.is_dir():
        print(f"Error: Path '{directory}' is not a directory")
        return 1
    
    # Find all compressed files
    files = find_compressed_files(directory)
    
    if not files:
        print(f"No .gz, .zstd, or .zst files found in '{directory}' or its subdirectories")
        return 0
    
    print(f"Found {len(files)} compressed files to process recursively")
    
    # Process files in parallel
    total_words = 0
    total_lines = 0
    results = []
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(count_words_in_file, filepath): filepath
            for filepath in files
        }
        
        # Process results as they complete
        with tqdm(total=len(files), desc="Processing files") as pbar:
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                try:
                    filename, word_count, line_count = future.result()
                    total_words += word_count
                    total_lines += line_count
                    results.append((filename, word_count, line_count))
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")
                finally:
                    pbar.update(1)
    
    # Print summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Location: {directory}")
    print(f"Total shard files: {len(files)}")
    print(f"Total documents: {total_lines:,}")
    print(f"Total words: {total_words:,}")

    print(f"Total estimate tokens (2.5 tok/word): {total_words*5//4:,}")
    
    return 0


if __name__ == "__main__":
    exit(main())