#!/usr/bin/env python

from subprocess import run

sections = [
    ("CLI", "tiledbvcf -h"),
    ("Create", "tiledbvcf create -h"),
    ("Store", "tiledbvcf store -h"),
    ("Delete", "tiledbvcf delete -h"),
    ("Export", "tiledbvcf export -h"),
    ("List", "tiledbvcf list -h"),
    ("Stat", "tiledbvcf stat -h"),
    ("Consolidate", "tiledbvcf utils consolidate -h"),
    ("Vacuum", "tiledbvcf utils vacuum -h"),
]

for section, cmd in sections:
    help = run(cmd.split(), capture_output=True, text=True).stdout.strip()
    if section == "CLI":
        print("# " + section)
    else:
        print("## " + section)
    print(f"\n```\n{help}\n```\n")
