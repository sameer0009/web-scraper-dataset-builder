@echo off
REM Web Scraper & Dataset Builder - Windows Installation and Run Script
REM This script automatically installs dependencies and runs the application

echo ========================================
echo Web Scraper ^& Dataset Builder
echo Automated Installation and Launch
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://python.org
    echo Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)

echo ✓ Python found
python --version

REM Check Python version (basic check)
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Python version: %PYTHON_VERSION%

REM Create virtual environment if it doesn't exist
if not exist "venv" (
    echo.
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo ERROR: Failed to create virtual environment
        echo Make sure you have the venv module installed
        pause
        exit /b 1
    )
    echo ✓ Virtual environment created
) else (
    echo ✓ Virtual environment already exists
)

REM Activate virtual environment
echo.
echo Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)
echo ✓ Virtual environment activated

REM Upgrade pip
echo.
echo Upgrading pip...
python -m pip install --upgrade pip
if errorlevel 1 (
    echo WARNING: Failed to upgrade pip, continuing anyway...
)

REM Install requirements
echo.
echo Installing dependencies...
echo This may take a few minutes...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    echo Please check your internet connection and try again
    pause
    exit /b 1
)
echo ✓ Dependencies installed successfully

REM Run basic tests to verify installation
echo.
echo Running basic tests to verify installation...
python test_basic.py
if errorlevel 1 (
    echo WARNING: Basic tests failed, but attempting to run application anyway...
    echo You may experience issues during operation
    timeout /t 3 >nul
) else (
    echo ✓ Basic tests passed
)

REM Launch the application
echo.
echo ========================================
echo Launching Web Scraper ^& Dataset Builder
echo ========================================
echo.
echo The application window should open shortly...
echo If you encounter any issues, check the console output above
echo.
echo To run the application again later, simply double-click this file
echo or run: python main.py
echo.

python main.py

REM Keep window open if there was an error
if errorlevel 1 (
    echo.
    echo ========================================
    echo Application exited with an error
    echo ========================================
    echo Please check the error messages above
    echo.
    pause
)

echo.
echo Application closed successfully
echo Thank you for using Web Scraper ^& Dataset Builder!
timeout /t 3 >nul