#!/usr/bin/env python3

import argparse
import hashlib
import os
import re
from fileinput import FileInput
from urllib.request import urlopen


def hash_url_file(url):
    """Return SHA1 hash of the file located at the provided url."""

    BLOCK_SIZE = 65536
    hash = hashlib.sha1()
    with urlopen(url) as fp:
        while True:
            data = fp.read(BLOCK_SIZE)
            if not data:
                return hash.hexdigest()
            hash.update(data)


def main(args):
    old_version = None
    old_hash = None
    sha1 = None

    filepath = (
        f"{os.path.dirname(__file__)}/../libtiledbvcf/cmake/Modules/FindTileDB_EP.cmake"
    )
    filepath = os.path.realpath(filepath)
    print(f"Updating {filepath}")

    with FileInput(filepath, inplace=True) as fp:
        for line in fp:
            line = line.rstrip()

            if old_version is None:
                m = re.search(r"TileDB/releases/download/(.*?)/.*-(.*)\.zip", line)
                if m:
                    old_version = m.group(1)
                    old_hash = m.group(2)

            if old_version is not None:
                # modify url
                if "https://" in line:
                    line = line.replace(old_version, args.version)
                    line = line.replace(old_hash, args.hash)

                # update sha1 value computed on previous line
                if sha1 is not None:
                    if "URL_HASH" in line:
                        line = re.sub(r"SHA1=.*", f"SHA1={sha1}", line)
                    else:
                        line = re.sub(r'".*"', f'"{sha1}"', line)
                    sha1 = None
                else:
                    m = re.search(r'"(https://.*)"', line)
                    if m:
                        sha1 = hash_url_file(m.group(1))

            # print line to file
            print(line)


if __name__ == "__main__":
    description = "Update FindTileDB_EP.cmake with a new TileDB version and hash"
    epilog = f"Example: {__file__} 2.5.4 eef8309"
    parser = argparse.ArgumentParser(description=description, epilog=epilog)
    parser.add_argument("version", help="new TileDB version")
    parser.add_argument("hash", help="new TileDB hash")
    args = parser.parse_args()

    main(args)
