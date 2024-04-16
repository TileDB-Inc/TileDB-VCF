@echo on

cd apis\python

set LIBTILEDBVCF_PATH="%CONDA_PREFIX%\Library"

pip install -v .[test]
if %ERRORLEVEL% neq 0 exit 1
