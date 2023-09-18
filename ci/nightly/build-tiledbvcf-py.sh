#!/bin/bash
set -ex

# Build (and test) tiledbvcf-py assuming source code directory is
# ./TileDB-VCF/apis/python

export LD_LIBRARY_PATH=$GITHUB_WORKSPACE/install/lib:${LD_LIBRARY_PATH-}
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

cd TileDB-VCF/apis/python
python setup.py install --libtiledbvcf=$GITHUB_WORKSPACE/install/
python -c "import tiledbvcf; print(tiledbvcf.version)"

pytest
