import argparse
import os
import traceback

from huggingface_hub import login, snapshot_download
from tqdm import tqdm


def download_data(
    huggingface_path, local_path, cache_dir, allow_patterns=None, max_workers=8
):
    while True:
        try:
            snapshot_download(
                huggingface_path,
                local_dir=local_path,
                cache_dir=cache_dir,
                repo_type="dataset",
                local_dir_use_symlinks=True,
                allow_patterns=allow_patterns,
                max_workers=max_workers,
            )
            break
        except KeyboardInterrupt:
            break
        except:
            traceback.print_exc()
            continue


def main():
    pass


if __name__ == "__main__":
    token = os.getenv("HF_TOKEN")
    if token:
        login(token=token)

    parser = argparse.ArgumentParser(description="Download HF dataset")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--loc", required=True)
    parser.add_argument("--allow-patterns", type=str)  # just one
    parser.add_argument("--cache-dir", default="/mnt/raid0/cache/")
    parser.add_argument("--max-workers", type=int)

    args = parser.parse_args()

    if args.allow_patterns != None:
        args.allow_patterns = [args.allow_patterns]
    if args.max_workers == None:
        args.max_workers = os.cpu_count() - 1

    download_data(
        args.dataset,
        args.loc,
        args.cache_dir,
        allow_patterns=args.allow_patterns,
        max_workers=args.max_workers,
    )
