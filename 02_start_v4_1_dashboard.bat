@echo off
cd /d "%~dp0"
title V4.1 Edge-Assisted DR Research Dashboard
echo =================================================================
echo V4.1 EDGE-ASSISTED DISASTER RECOVERY RESEARCH DASHBOARD
echo =================================================================
echo Dashboard: http://127.0.0.1:5050
echo Keep this window open while using the prototype.
echo Run 01_install_requirements.bat first on a new machine.
echo =================================================================
python dashboard_app.py
if errorlevel 1 (
  echo.
  echo The dashboard stopped with an error.
  echo Check dashboard_logs and confirm dependencies are installed.
)
pause
