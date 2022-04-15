# Initialize pyarrow first.
import pyarrow

import ctypes
import os
import sys


def _load_libs():
    """Loads the required TileDB-VCF native library."""
    if sys.platform == "darwin":
        lib_name = "libtiledbvcf.dylib"
    else:
        lib_name = "libtiledbvcf.so"

    try:
        # Try loading the bundled native library.
        lib_dir = os.path.dirname(os.path.abspath(__file__))
        ctypes.CDLL(os.path.join(lib_dir, lib_name))
    except OSError as e:
        # Otherwise try loading by name only.
        ctypes.CDLL(lib_name)


# Load native libraries
_load_libs()

# Load basic modules first
from .dataset import ReadConfig, TileDBVCFDataset, Dataset, config_logging

# Load dask additions, if dask is available
try:
    import dask
    from . import dask_functions
except ImportError:
    pass

from .version import version
