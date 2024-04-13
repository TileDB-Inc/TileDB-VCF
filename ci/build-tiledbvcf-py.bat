@echo on

cd apis\python

set LIBTILEDBVCF_DIR="%CONDA_PREFIX%\Library"

pip install -v . 
if %ERRORLEVEL% neq 0 exit 1
