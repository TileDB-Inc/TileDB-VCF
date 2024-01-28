@echo on

rem Build libtiledb assuming source code directory is .\TileDB

cmake -S TileDB -B build-libtiledb ^
  -D CMAKE_BUILD_TYPE=Release ^
  -D CMAKE_INSTALL_PREFIX:PATH=%GITHUB_WORKSPACE%\install\ ^
  -D TILEDB_WERROR=ON ^
  -D TILEDB_SERIALIZATION=ON ^
  -D TILEDB_VCPKG=OFF ^
  -D TILEDB_S3=ON
if %ERRORLEVEL% neq 0 exit 1

cmake --build build-libtiledb -j2 --config Release
if %ERRORLEVEL% neq 0 exit 1

cmake --build build-libtiledb --config Release --target install-tiledb
if %ERRORLEVEL% neq 0 exit 1
