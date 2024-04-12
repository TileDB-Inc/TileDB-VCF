@echo on

cd apis\python

pip install -v . 
if %ERRORLEVEL% neq 0 exit 1
