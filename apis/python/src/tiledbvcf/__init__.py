# Initialize pyarrow first.
import pyarrow
import pyarrow_hotfix

import ctypes
import os
import sys

import tiledbvcf.libtiledbvcf

# Load basic modules first
from .dataset import ReadConfig, TileDBVCFDataset, Dataset, config_logging

# Load dask additions, if dask is available
try:
    import dask
    from . import dask_functions
except ImportError:
    pass

from .version import version
from .allele_frequency import read_allele_frequency
