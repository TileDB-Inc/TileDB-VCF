# TileDB-VCF

Efficient variant-call data storage and retrieval library using the TileDB storage library.

# Building

## Dependencies

CMake is used to build the native library, all required dependencies, and the APIs.

HTSlib version >= 1.8 is a required dependency. Additionally, if HTSlib is not installed on your system, you must install automake and autoconf, which are used when building a local copy of HTSlib:
* Linux: `sudo apt install autoconf automake`
* macOS: `brew install autoconf`
