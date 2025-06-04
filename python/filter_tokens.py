#!/usr/bin/env python3
"""
Script to filter JSONL files based on a denylist, removing entries where metadata.Source-File matches.
Filtered files are recompressed with zstd and saved to a destination directory preserving folder structure.
Usage: python filter_tokens.py /path/to/source /path/to/destination --denylist denylist.txt
"""

import argparse
import gzip
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Set, Tuple

try:
    import zstandard as zstd
except ImportError:
    print("Error: zstandard is required. Install with: pip install zstandard")
    exit(1)

from tqdm import tqdm


def load_denylist(denylist_path: Path) -> Set[str]:
    """Load denylist from text file into a set for fast lookups."""
    denylist = set()
    try:
        with open(denylist_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    denylist.add(line)
    except Exception as e:
        print(f"Error loading denylist: {e}")
        exit(1)
    
    return denylist


def process_file(args: Tuple[Path, Path, Path, Set[str]]) -> Tuple[str, int, int, int]:
    """
    Process a single compressed file, filter based on denylist, and save to destination.
    
    Args:
        args: Tuple of (source_file, source_root, dest_root, denylist)
    
    Returns:
        Tuple of (filename, total_lines, filtered_lines, words_kept)
    """
    source_file, source_root, dest_root, denylist = args
    
    total_lines = 0
    filtered_lines = 0
    words_kept = 0
    
    try:
        # Calculate destination path preserving folder structure
        relative_path = source_file.relative_to(source_root)
        # Change extension to .zst for output
        dest_path = dest_root / relative_path.with_suffix('.zst')
        
        # Create destination directory if needed
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Open source file based on extension
        if source_file.suffix == '.gz':
            source_open = gzip.open(source_file, 'rt')
        elif source_file.suffix in ['.zstd', '.zst']:
            dctx = zstd.ZstdDecompressor()
            source_open = dctx.stream_reader(open(source_file, 'rb'), closefd=True)
            source_open = source_open.read1().decode('utf-8').splitlines()
        else:
            return (str(source_file), 0, 0, 0)
        
        # Open destination file with zstd compression
        cctx = zstd.ZstdCompressor(level=3)  # Default compression level
        
        with open(dest_path, 'wb') as dest_file:
            with cctx.stream_writer(dest_file) as compressor:
                # Process source file
                if source_file.suffix == '.gz':
                    lines_iter = source_open
                else:
                    # For zstd files, we need to properly handle the decompression
                    dctx = zstd.ZstdDecompressor()
                    with open(source_file, 'rb') as f:
                        with dctx.stream_reader(f) as reader:
                            text_data = reader.read().decode('utf-8')
                            lines_iter = text_data.splitlines()
                
                for line in lines_iter:
                    total_lines += 1
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        
                        # Check if metadata.Source-File is in denylist
                        metadata = data.get('metadata', {})
                        source_file_name = metadata.get('Source-File', '')
                        
                        if source_file_name in denylist:
                            filtered_lines += 1
                            continue
                        
                        # Keep this entry - write to output
                        compressor.write((json.dumps(data, ensure_ascii=False) + '\n').encode('utf-8'))
                        
                        # Count words in kept entries
                        text = data.get('text', '')
                        if text:
                            words_kept += len(text.split())
                            
                    except json.JSONDecodeError:
                        # Skip invalid JSON lines
                        continue
                    except Exception as e:
                        # Skip problematic lines
                        continue
        
        # Close source file if it's gzip
        if source_file.suffix == '.gz':
            source_open.close()
                    
    except Exception as e:
        print(f"Error processing {source_file}: {e}")
        return (str(source_file), 0, 0, 0)
    
    return (str(source_file), total_lines, filtered_lines, words_kept)


def find_compressed_files(directory: Path) -> list[Path]:
    """Find all .gz, .zstd, and .zst files recursively in the given directory."""
    extensions = ['.gz', '.zstd', '.zst']
    files = []
    
    for ext in extensions:
        files.extend(directory.rglob(f'*{ext}'))
    
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(
        description='Filter compressed JSONL files based on metadata.Source-File denylist'
    )
    parser.add_argument(
        'source',
        type=str,
        help='Source directory containing compressed files'
    )
    parser.add_argument(
        'destination',
        type=str,
        help='Destination directory for filtered files'
    )
    parser.add_argument(
        '-d', '--denylist',
        type=str,
        required=True,
        help='Path to denylist text file (one Source-File per line)'
    )
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=None,
        help='Number of worker processes (default: number of CPU cores)'
    )
    
    args = parser.parse_args()
    
    # Validate paths
    source_dir = Path(args.source)
    dest_dir = Path(args.destination)
    denylist_path = Path(args.denylist)
    
    if not source_dir.exists():
        print(f"Error: Source path '{source_dir}' does not exist")
        return 1
    
    if not source_dir.is_dir():
        print(f"Error: Source path '{source_dir}' is not a directory")
        return 1
    
    if not denylist_path.exists():
        print(f"Error: Denylist file '{denylist_path}' does not exist")
        return 1
    
    # Create destination directory if it doesn't exist
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Load denylist
    print(f"Loading denylist from '{denylist_path}'...")
    denylist = load_denylist(denylist_path)
    print(f"Loaded {len(denylist)} entries in denylist")
    
    # Find all compressed files
    files = find_compressed_files(source_dir)
    
    if not files:
        print(f"No .gz, .zstd, or .zst files found in '{source_dir}' or its subdirectories")
        return 0
    
    print(f"Found {len(files)} compressed files to process")
    print(f"Output directory: {dest_dir}")
    
    # Process files in parallel
    total_lines_processed = 0
    total_lines_filtered = 0
    total_words_kept = 0
    results = []
    
    # Prepare arguments for parallel processing
    process_args = [(f, source_dir, dest_dir, denylist) for f in files]
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_file, arg): arg[0]
            for arg in process_args
        }
        
        # Process results as they complete
        with tqdm(total=len(files), desc="Processing files") as pbar:
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                try:
                    filename, total_lines, filtered_lines, words_kept = future.result()
                    total_lines_processed += total_lines
                    total_lines_filtered += filtered_lines
                    total_words_kept += words_kept
                    results.append((filename, total_lines, filtered_lines, words_kept))
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")
                finally:
                    pbar.update(1)
    
    # Print summary
    print("\n" + "="*60)
    print("FILTERING SUMMARY")
    print("="*60)
    print(f"Source directory: {source_dir}")
    print(f"Destination directory: {dest_dir}")
    print(f"Denylist file: {denylist_path}")
    print(f"Denylist entries: {len(denylist)}")
    print("-"*60)
    print(f"Total files processed: {len(files)}")
    print(f"Total documents processed: {total_lines_processed:,}")
    print(f"Total documents filtered out: {total_lines_filtered:,}")
    print(f"Total documents kept: {total_lines_processed - total_lines_filtered:,}")
    print(f"Filtering rate: {total_lines_filtered/total_lines_processed*100:.2f}%" if total_lines_processed > 0 else "N/A")
    print("-"*60)
    print(f"Total words in kept documents: {total_words_kept:,}")
    print(f"Estimated tokens (1.25 tok/word): {total_words_kept * 5 // 4:,}")
    
    return 0


if __name__ == "__main__":
    exit(main())