""" One-off script for jiacheng to make a bunch of S5CMD run scripts that:
	- take an input directory on s3
	- collect all files and shuffles them 
	- creates output scripts that have total max_size < LIMIT 
"""

import fcntl
import io
import os
import random
import re
import select
import subprocess
import sys
import threading
import time
from tempfile import NamedTemporaryFile
from typing import Callable, Dict, List, Optional, Tuple, Union

import boto3
import click
from tqdm.auto import tqdm

import s5cmd


@click.group()
def cli():
    pass


@cli.command()
@click.option("--s3-src", required=True)
@click.option("--remote-dst", required=True)
@click.option("--script-output", required=True)
@click.option("--limit", required=True)
def make_download_scripts(s3_src, remote_dst, script_output, limit):
    files = s5cmd.S5CMD().ls(os.path.join(s3_src, "*"))
    random.shuffle(files)

    groups = []
    cur_size = 0
    cur_group = []
    limit = int(limit)
    for el in files:
        if cur_size + el["size"] > limit:
            groups.append(cur_group)
            cur_group = []
            cur_size = 0
        cur_group.append(el)
        cur_size += el["size"]
    groups.append(cur_group)

    os.makedirs(script_output, exist_ok=True)

    line_getter = lambda d: "cp %s %s" % (
        d["name"],
        os.path.join(remote_dst, d["name"].replace(s3_src, "").lstrip("/")),
    )

    for i, group in enumerate(groups):
        contents = "\n".join(line_getter(_) for _ in group)
        with open(
            os.path.join(script_output, "downloader_part_%04d.txt" % i), "w"
        ) as f:
            f.write(contents)


if __name__ == "__main__":
    cli()
