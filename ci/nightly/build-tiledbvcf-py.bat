@echo on

rem Build (and test) tiledbvcf-py assuming source code directory is
rem .\TileDB-VCF\apis\python

set PATH=%GITHUB_WORKSPACE%\install\bin;%PATH%
echo "PATH: %PATH%"

cd TileDB-VCF\apis\python
python setup.py develop --libtiledbvcf=%GITHUB_WORKSPACE%\install\
if %ERRORLEVEL% neq 0 exit 1

python -c "import tiledbvcf; print(tiledbvcf.version)"
if %ERRORLEVEL% neq 0 exit 1

pytest
if %ERRORLEVEL% neq 0 exit 1
