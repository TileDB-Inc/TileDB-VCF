@echo on

rem Build libtiledbvcf assuming source code directory is .\TileDB-VCF\libtiledbvcf

cmake -S TileDB-VCF\libtiledbvcf -B build-libtiledbvcf ^
  -D CMAKE_BUILD_TYPE=Release ^
  -D CMAKE_INSTALL_PREFIX:PATH=%GITHUB_WORKSPACE%\install\ ^
  -D OVERRIDE_INSTALL_PREFIX=OFF ^
  -D TILEDB_WERROR=OFF ^
  -D CMAKE_INSTALL_LIBDIR=bin
if %ERRORLEVEL% neq 0 exit 1

cmake --build build-libtiledbvcf -j2 --config Release
if %ERRORLEVEL% neq 0 exit 1

cmake --build build-libtiledbvcf --config Release --target install-libtiledbvcf
if %ERRORLEVEL% neq 0 exit 1

dir /s %GITHUB_WORKSPACE%\install\

rem %GITHUB_WORKSPACE%\install\bin\tiledbvcf version
rem if %ERRORLEVEL% neq 0 exit 1
