@echo off
REM BYD/Valley Daily Data Import - Batch Launcher
REM This script runs the daily import process and launches the dashboard

REM Set UTF-8 code page for proper Unicode symbol display
chcp 65001 >nul 2>&1

cd /d "%~dp0"

echo ========================================
echo BYD/Valley Daily Data Import
echo ========================================
echo.

REM Set Python to use UTF-8 encoding
set PYTHONIOENCODING=utf-8

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
)

REM Run the daily import script with dashboard launch
python v2\daily_import.py --launch-app

REM Keep window open if there's an error
if errorlevel 1 (
    echo.
    echo ========================================
    echo An error occurred. Press any key to exit.
    echo ========================================
    pause > nul
)
