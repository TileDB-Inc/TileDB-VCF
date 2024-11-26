@echo on

mkdir libtiledbvcf-build
cd libtiledbvcf-build

rem configure
cmake ^
  -DCMAKE_INSTALL_PREFIX:PATH="%CONDA_PREFIX%\Library" ^
  -DOVERRIDE_INSTALL_PREFIX=OFF ^
  -DCMAKE_BUILD_TYPE=Release ^
  -DFORCE_EXTERNAL_HTSLIB=OFF ^
  ../libtiledbvcf
if %ERRORLEVEL% neq 0 exit 1

rem build
cmake --build . --config Release --parallel %CPU_COUNT%
if %ERRORLEVEL% neq 0 exit 1

rem install
cmake --build . --target install --config Release
if %ERRORLEVEL% neq 0 exit 1
