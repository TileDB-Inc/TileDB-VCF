#!/usr/bin/env python3

import argparse
import hashlib
import os
import re
from subprocess import run
from urllib.request import urlopen


def hash_url_file(url):
    """Return SHA1 hash of the file located at the provided url."""

    try:
        print(f"  hashing {url}")
        BLOCK_SIZE = 65536
        hash = hashlib.sha1()
        with urlopen(url) as fp:
            while True:
                data = fp.read(BLOCK_SIZE)
                if not data:
                    return hash.hexdigest()
                hash.update(data)
    except Exception as e:
        print(f"Error: failed to hash file at {url}")
        raise


def get_version_hash(version):
    cmd = "git ls-remote --tags https://github.com/TileDB-Inc/TileDB.git"
    output = run(cmd, shell=True, capture_output=True).stdout.decode()

    m = re.search(rf"\s(\S+)\s+refs/tags/{version}\s", output)
    if m:
        return m.group(1)[0:7]

    print(output)
    print(f"Error: version {version} not found.")
    exit(1)


def main(args):
    old_version = None
    old_hash = None
    sha1 = None

    new_version = args.version
    new_hash = get_version_hash(new_version)

    filepath = (
        f"{os.path.dirname(__file__)}/../libtiledbvcf/cmake/Modules/FindTileDB_EP.cmake"
    )
    print(f"Updating {filepath}")
    print(f"  new version = {new_version}-{new_hash}")

    findtiledb_lines = []
    with open(filepath) as fp:
        for line in fp:
            line = line.rstrip()

            # Find the old version and hash value
            if old_version is None:
                m = re.search(r"TileDB/releases/download/(.*?)/.*-(.*)\.zip", line)
                if m:
                    old_version = m.group(1)
                    old_hash = m.group(2)
                    print(f"  old version = {old_version}-{old_hash}")

            if old_version is not None:
                # modify url
                if "https://" in line:
                    line = line.replace(old_version, new_version)
                    line = line.replace(old_hash, new_hash)

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

            # Add line to the list of lines to write
            findtiledb_lines.append(line)

    # Write new lines to file
    with open(filepath, "w") as fp:
        for line in findtiledb_lines:
            print(line, file=fp)


if __name__ == "__main__":
    description = "Update FindTileDB_EP.cmake with a new TileDB version"
    epilog = f"Example: {__file__} 2.5.4"
    parser = argparse.ArgumentParser(description=description, epilog=epilog)
    parser.add_argument("version", help="new TileDB version")
    args = parser.parse_args()

    main(args)
