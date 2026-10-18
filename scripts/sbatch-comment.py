#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import re
import sys

sbatch_pattern = r"^\s*#SBATCH"
comment_pattern = r'--comment=(["\'])(.*?)\1'


def replace_sbatch_comment(input_file, output_file):
    sbatch = re.compile(sbatch_pattern)
    comment = re.compile(comment_pattern)
    newlines = []
    first_sbatch = None
    did_comment = False
    # Read the input file
    with open(input_file, "r") as file:
        lines = file.readlines()
        for ct, line in enumerate(lines):
            line = line.rstrip()
            outline = line
            if sbatch.match(line) is not None:
                if first_sbatch is None:
                    first_sbatch = ct
                if (comment_match := comment.search(line)) is not None:
                    original_comment = comment_match.group(2)
                    try:
                        json_comment = json.loads(original_comment)
                        if "APS" not in json_comment:
                            json_comment["APS"] = {}
                    except json.decoder.JSONDecodeError:
                        json_comment = {"APS": {}, "original_comment": original_comment}
                    new_comment = json.dumps(json_comment)
                    outline = line[: comment_match.start(2) - 1] + f"'{new_comment}'" + line[comment_match.end(2) + 1 :]
                    did_comment = True
            newlines.append(outline)
    if not did_comment:
        if first_sbatch is None:
            raise RuntimeError("I can't find any sbatch lines, I'm giving up")
        json_comment = {"APS": {}}
        new_comment = json.dumps(json_comment)
        newlines[first_sbatch] = newlines[first_sbatch] + f" --comment='{new_comment}'"
    with open(output_file, "w") as file:
        file.write("\n".join(newlines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("infile", help="Input sbatch file")

    parser.add_argument("outfile", help="Output sbatch file")
    args = parser.parse_args(sys.argv[1:])

    replace_sbatch_comment(args.infile, args.outfile)
