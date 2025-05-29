import sys

INTERVAL = 4096
LIMIT = 32768


def main(idx, part):
    assert 0 <= idx < 8
    assert part in ["a", "b"]
    with open("download_script.txt", "w") as f:
        for i in range(INTERVAL * idx, INTERVAL * idx + INTERVAL):
            assert i < LIMIT
            f.write(
                "cp s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v3_subsamples/ed_sub0.25_minhash2x_2611/groups/part_%s/chunk_%08d.* /mnt/raid0/groups/ \n"
                % (part, i)
            )


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2])
