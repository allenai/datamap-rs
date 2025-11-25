import glob
import json
import os
import random
import re
import time
from collections import Counter, defaultdict
from functools import partial
from multiprocessing import Pool

import yaml
import zstandard
from ruamel.yaml import YAML
from tqdm import tqdm

reader = lambda x: [
    json.loads(_)
    for _ in zstandard.ZstdDecompressor()
    .stream_reader(open(x, "rb").read())
    .read()
    .splitlines()
]


def get_yaml_id_map(yaml_file):
    yaml_obj = yaml.safe_load(open(yaml_file))
    lookup = {}
    for i, obj in enumerate(yaml_obj["pipeline"]):
        lookup[i] = obj.get("id", None)
    return lookup


def parse_timing_logs(logfile):
    logdata = open(logfile, "r").read()
    steps = [
        float(_.group().split(" ")[-1])
        for _ in list(re.finditer(r"Step \d+ .+?\n\s+Spent\s+\d+\.\d{2}", logdata))
    ]
    return {i: el for i, el in enumerate(steps)}


def get_annos(f):
    data = reader(f)
    counts = dict(
        Counter(
            tuple(sorted(int(x) for x in _["metadata"].get("filter_anno", {})))
            for _ in data
        )
    )
    return counts


def calculate_total_time(order, annos, timing):
    """Calculate total execution time for a given filter order"""
    total_time = 0

    survivors = {k: v for k, v in annos.items() if len(k) > 0}

    for filter_id in order:
        docs_to_process = sum(survivors.values())
        total_time += docs_to_process * timing[filter_id]
        # Remove documents that this filter catches
        survivors = {k: v for k, v in survivors.items() if filter_id not in k}

    return total_time / sum(annos.values())


def modify_yaml_order(yaml_file, new_order, output_file):
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False

    data = yaml.load(open(yaml_file))
    cur_pipeline = data["pipeline"]
    new_pipeline = [cur_pipeline[i] for i in new_order]
    data["pipeline"] = new_pipeline
    with open(output_file, "w") as f:
        yaml.dump(data, f)
