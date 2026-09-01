@echo off
cd /d "%~dp0"
echo Installing V4.1 research prototype requirements...
python -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo Installation failed. Confirm that Python is installed, added to PATH, and internet access is available for pip.
  pause
  exit /b 1
)
echo.
echo V4.1 requirements installed successfully.
pause
