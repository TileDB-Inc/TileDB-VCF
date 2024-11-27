#!/bin/bash
set -ex

# Build (and test) tiledbvcf-py assuming source code directory is
# ./TileDB-VCF/apis/python and libtiledbvcf shared library installed in
# $GITHUB_WORKSPACE/install/

OS=$(uname)
echo "OS: $OS"
if [[ $OS == Linux ]]
then
  export LD_LIBRARY_PATH=$GITHUB_WORKSPACE/install/lib:${LD_LIBRARY_PATH-}
  echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
elif [[ $OS == Darwin ]]
then
  export DYLD_LIBRARY_PATH=$GITHUB_WORKSPACE/install/lib:${DYLD_LIBRARY_PATH-}
  echo "DYLD_LIBRARY_PATH: $DYLD_LIBRARY_PATH"
fi

export tiledbvcf_DIR=$GITHUB_WORKSPACE/install/
export TileDB_DIR=$GITHUB_WORKSPACE/install/

cd TileDB-VCF
python -m pip install -v --no-deps --config-settings=cmake.define.TILEDBVCF_ONLY_PYTHON_BINDINGS=ON .[test]
python -c "import tiledbvcf; print(tiledbvcf.version)"

pytest
