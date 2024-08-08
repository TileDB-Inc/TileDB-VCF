#!/bin/bash
set -ex

# Build tiledb-py assuming source code directory is ./TileDB-Py/ and libtiledb
# shared library installed in $GITHUB_WORKSPACE/install/

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

export TILEDB_PATH=$GITHUB_WORKSPACE/install/

cd TileDB-Py/
python -m pip install -Cskbuild.cmake.define.TILEDB_REMOVE_DEPRECATIONS=OFF -v .

python -c "import tiledb; print('successful import')"
python -c "import tiledb; print(tiledb.libtiledb.version())"
python -c "import tiledb; print(tiledb.version())"
