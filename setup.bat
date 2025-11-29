@echo off

echo Starting project setup...

if not exist venv (
    python -m venv venv
    echo Virtual environment created.
)

call venv\Scripts\activate

echo Installing and upgrading pip...
python.exe -m pip install --upgrade pip > NUL 2>&1

echo Installing requirements...
python.exe -m pip install -r requirements.txt > NUL 2>&1

echo Launching main.py...
python main.py

:END
echo.
echo Script finished or encountered an error.
pause