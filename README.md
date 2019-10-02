# TileDB-VCF

Efficient variant-call data storage and retrieval library using the TileDB storage library.

# Building

## Dependencies

CMake is required to build the native library, all required dependencies, and the APIs.

HTSlib version >= 1.8 is a required dependency. Additionally, unless HTSlib is already installed on your system, you must install automake and autoconf, which are used when building a local copy of HTSlib:
* Linux: `sudo apt install autoconf automake` or `sudo yum install automake`
* macOS: `brew install autoconf`

TileDB is also a required dependency, and will be built automatically if not already installed on your system.

### AWS Linux

To build on AWS Linux (such as for a Spark environment) you may need a few extra steps.

Install a newer gcc version:
```bash
sudo yum install -y epel-release gcc gcc-c++
sudo yum install -y centos-release-scl
sudo yum install -y devtoolset-7-gcc
sudo yum install -y devtoolset-7-gcc-c++
scl enable devtoolset-7 bash
```

Install recent CMake:
```bash
wget https://cmake.org/files/v3.12/cmake-3.12.3-Linux-x86_64.sh
sudo sh cmake-3.12.3-Linux-x86_64.sh --skip-license --prefix=/usr/local/
```

Required dependencies:
```bash
sudo yum install -y git automake
```

A few extra dependencies to speed up the build:
```bash
sudo yum install -y git automake
```

## Python

When building the Python API from source, `conda` is used for package dependencies. To install the Python `tiledbvcf` module:
```bash
cd apis/python
conda create -f conda-env.yml
conda activate tiledbvcf-py
python setup.py install
```
(note it is not required to build the native library/C API first, the `setup.py` script will do this automatically.)

To run the Python tests:
```python
python setup.py pytest
```

You should now be able to import the module as normal:
```python
import tiledbvcf
```

## Spark

To build the Spark API:
```bash
cd apis/spark
./gradlew assemble
```
(note it is not required to build the native library/C API first, the `build.gradle` script will do this automatically.)

To run the tests:
```bash
./gradlew test
```

To build a jar, including bundled native libraries:
```bash
./gradlew jar
```
The output `.jar` will be created in the `build/libs/` directory.

## C

If you are interested in only building the underlying TileDB-VCF library and C API:

```bash
$ cd libtiledbvcf
$ mkdir build && cd build
$ cmake .. && make -j4
$ make install-libtiledbvcf
```

To run the tests:
```bash
$ make check
```

To run the CLI client tests:
```bash
$ ../test/run-cli-tests.sh . ../test/inputs
```