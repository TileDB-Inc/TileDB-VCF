#!/bin/bash
set -ex

# Build (and test) tiledbvcf-py assuming source code directory is
# ./TileDB-VCF/apis/python and libtiledbvcf shared library installed in
# $GITHUB_WORKSPACE/install/

export LD_LIBRARY_PATH=$GITHUB_WORKSPACE/install/lib:${LD_LIBRARY_PATH-}
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

export LIBTILEDBVCF_PATH=$GITHUB_WORKSPACE/install/

cd TileDB-VCF/apis/python
python setup.py install
python -c "import tiledbvcf; print(tiledbvcf.version)"

pytest
