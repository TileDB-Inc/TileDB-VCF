# Initialize pyarrow first.
import pyarrow

import ctypes
import os
import sys

if sys.platform == 'darwin':
    lib_name = 'libtiledbvcf.dylib'
else:
    lib_name = 'libtiledbvcf.so'

try:
    # Try loading the bundled native library.
    lib_dir = os.path.dirname(os.path.abspath(__file__))
    ctypes.CDLL(os.path.join(lib_dir, lib_name))
except OSError as e:
    # Otherwise try loading by name only.
    ctypes.CDLL(lib_name)

from .dataset import (ReadConfig, TileDBVCFDataset)
