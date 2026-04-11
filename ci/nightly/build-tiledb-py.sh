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

# Pin pybind11 version if PYBIND11_MAX_VERSION is set (e.g. "<3")
if [ -n "${PYBIND11_MAX_VERSION:-}" ]; then
  echo "pybind11${PYBIND11_MAX_VERSION}" > /tmp/pybind11-constraint.txt
  export PIP_CONSTRAINT=/tmp/pybind11-constraint.txt
fi

cd TileDB-Py/
python -m pip install -Cskbuild.cmake.define.TILEDB_REMOVE_DEPRECATIONS=OFF -v .

# Can't run the import inside of the Git repo because Python automatically looks
# for `./module/__init.py__`
cd ..
python -c "import tiledb; print('successful import')"
python -c "import tiledb; print(tiledb.libtiledb.version())"
python -c "import tiledb; print(tiledb.version())"
