# TileDB-VCF

Efficient variant-call data storage and retrieval library using the TileDB storage library.

# Building

## Dependencies

CMake is required to build the native library, all required dependencies, and the APIs.

HTSlib version >= 1.8 is a required dependency. Additionally, unless HTSlib is already installed on your system, you must install automake and autoconf, which are used when building a local copy of HTSlib:
* Linux: `sudo apt install autoconf automake`
* macOS: `brew install autoconf`

TileDB is also a required dependency, and will be built automatically if not already installed on your system.

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