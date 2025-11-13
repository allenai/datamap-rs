import argparse
import os
import traceback

from huggingface_hub import login, snapshot_download
from tqdm import tqdm


def download_model(huggingface_path, local_path, cache_dir, max_workers=8):
    while True:
        try:
            snapshot_download(
                huggingface_path,
                local_dir=local_path,
                local_dir_use_symlinks=True,
            )
            break
        except KeyboardInterrupt:
            break
        except:
            traceback.print_exc()
            continue

    pass


if __name__ == "__main__":
    token = os.getenv("HF_TOKEN")
    if token:
        login(token=token)

    parser = argparse.ArgumentParser(description="Download HF dataset")
    parser.add_argument("--model", required=True)
    parser.add_argument("--loc", required=True)
    parser.add_argument("--max-workers", type=int)

    args = parser.parse_args()
    if args.max_workers == None:
        args.max_workers = os.cpu_count() - 1

    download_data(
        args.dataset,
        args.loc,
        max_workers=args.max_workers,
    )
