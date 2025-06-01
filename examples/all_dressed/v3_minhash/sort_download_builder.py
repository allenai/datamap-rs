import sys

INCREMENT = 1024
PRES = ["%02d" % i for i in range(32)]


def main(idx):
    idx = int(idx)
    output_file = "/mnt/raid0/downloader_%02d.txt" % idx
    with open(output_file, "w") as f:
        for pre in PRES:
            for chunk_id in range(idx * INCREMENT, idx * INCREMENT + INCREMENT):
                line = (
                    "cp s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3/minhash/param_26_11/groups/%s/chunk_%08d.* /mnt\raid0/input/\n"
                    % (pre, chunk_id)
                )
                f.write(line)


if __name__ == "__main__":
    idx = sys.argv[1]
    main(idx)
