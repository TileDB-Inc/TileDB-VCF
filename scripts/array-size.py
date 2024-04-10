#!/usr/bin/env python

import numpy as np
import pandas as pd
import tiledb
import typer
from collections import defaultdict


app = typer.Typer()


def read_column_sizes(uri):
    column_sizes = defaultdict(int)

    vfs = tiledb.VFS()
    for frag in vfs.ls(f"{uri}/__fragments"):
        for file in vfs.ls(frag):
            key = file.split("/")[-1].split(".")[0]
            if key.startswith("__"):
                continue
            column_sizes[key] += vfs.file_size(file)

    return column_sizes


@app.command()
def query(
    uri: str,
    *,
    filter: bool = typer.Option(False, help="Report filter information"),
    to_csv: str = typer.Option(None, help="Save output to a CSV file"),
):
    """Report size of TileDB array."""

    with_filter = filter

    column_sizes = read_column_sizes(uri)

    def column_size(name, dtype, filter, id):
        size_data = column_sizes.get(id, 0)
        size_var = column_sizes.get(f"{id}_var", 0)
        size_validity = column_sizes.get(f"{id}_validity", 0)

        record = {}
        record["name"] = name
        record["type"] = dtype
        if with_filter:
            record["filter"] = filter
        record["data"] = size_var if size_var else size_data
        record["offsets"] = size_data if size_var else size_var
        record["validity"] = size_validity
        record["total"] = size_data + size_var + size_validity

        return record

    data = []

    with tiledb.open(uri) as A:
        for i in range(A.ndim):
            name = A.dim(i).name + "*"
            id = f"d{i}"
            dtype = np.dtype(A.dim(i).dtype).name
            filter = A.dim(i).filters
            data.append(column_size(name, dtype, filter, id))

        for i in range(A.nattr):
            name = A.attr(i).name
            id = f"a{i}"
            dtype = np.dtype(A.attr(i).dtype).name
            filter = A.attr(i).filters
            data.append(column_size(name, dtype, filter, id))

    df = pd.DataFrame(data)
    df["percent"] = (100 * df.total / df.total.sum()).round(3)

    df = df.sort_values("total", ascending=False).reset_index(drop=True)

    if to_csv:
        df.to_csv(to_csv, index=False)
    else:
        pd.set_option("display.max_colwidth", None)
        pd.set_option("display.max_rows", None)
        print(df)

        print(f"Total size: {bytes_to_human_readable(df.total.sum())}")


# write a function that converts bytes to KiB, MiB, GiB, etc.
def bytes_to_human_readable(size):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PiB"


if __name__ == "__main__":
    app()
