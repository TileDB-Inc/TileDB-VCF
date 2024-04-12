@echo on

cd apis\python

pip install . ^
  --single-version-externally-managed ^
  --record record.txt ^
  --libtiledbvcf="%CONDA_PREFIX%\Library"
if %ERRORLEVEL% neq 0 exit 1
