@echo on

pip install -v .[test]
if %ERRORLEVEL% neq 0 exit 1
