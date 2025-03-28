"""
Some python code to make sure things that we need for running a map command 

"""

import click
import fasttext
import yaml


@click.group()
def cli():
    pass


# ===================================================================
# =                          VERIFIERS                              =
# ===================================================================


def check_url_substring_filter(kwargs):
    banlist_file = kwargs["banlist_file"]
    try:
        open(banlist_file, "r").read()
    except:
        raise Exception("Could not find banlist file: %s" % banlist_file)


def check_fasttext_annotator(kwargs):
    fasttext.load_model(kwargs["fast_text_file"])


def check_madlad400(kwargs):
    fasttext.load_model(kwargs["fast_text_file"])
    try:
        open(kwargs["cursed_regex_file"], "r").read()
    except:
        raise Exception(
            "Could not find cursed regex file: %s" % kwargs["cursed_regex_file"]
        )


# ===================================================================
# =                          MAIN                                   =
# ===================================================================

VERIFY_CHECKERS = {
    "url_substring_filter": check_url_substring_filter,
    "fasttext_annotator": check_fasttext_annotator,
    "madlad400_sentence_filter": check_madlad400,
}


@cli.command()
@click.option("--config", required=True)
def verify(config):
    errs = []
    pipeline_steps = yaml.safe_load(open(config, "r"))["pipeline"]
    for step in pipeline_steps:
        if step["name"] not in VERIFY_CHECKERS:
            continue
        try:
            VERIFY_CHECKERS[step["name"]](step["kwargs"])
        except Exception as err:
            errs.append(err)
    if len(errs) == 0:
        print("No errors! Good to run the pipeline!")
    else:
        print("Found %s errors:" % len(errs))
        for err in errs:
            print(err)


if __name__ == "__main__":
    cli()
