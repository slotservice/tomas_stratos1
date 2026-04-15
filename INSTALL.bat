@echo off
chcp 65001 >nul 2>&1
title Stratos1 Trading Bot - Installation
color 0A

echo.
echo  ============================================================
echo       STRATOS1 TRADING BOT - AUTOMATIC INSTALLATION
echo  ============================================================
echo.
echo  This will install everything needed to run the bot.
echo  Just sit back and wait - it does everything for you.
echo.
echo  ============================================================
echo.
pause

set "PROJECT_DIR=%~dp0"
set "VENV_DIR=%PROJECT_DIR%venv"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"
set "PIP=%VENV_DIR%\Scripts\pip.exe"

REM ============================================================
REM Step 1: Check if Python is installed
REM ============================================================
echo.
echo  [Step 1/5] Checking if Python is installed...
echo.

where python >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python is NOT installed on this computer.
    echo.
    echo  Please install Python 3.12 or newer from:
    echo     https://www.python.org/downloads/
    echo.
    echo  IMPORTANT: During installation, check the box that says:
    echo     "Add Python to PATH"
    echo.
    echo  After installing Python, run this file again.
    echo.
    pause
    exit /b 1
)

python --version 2>&1
echo.
echo  Python found!
echo.

REM ============================================================
REM Step 2: Create virtual environment
REM ============================================================
echo  [Step 2/5] Creating virtual environment...
echo.

if exist "%VENV_DIR%" (
    echo  Virtual environment already exists - skipping.
) else (
    python -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo  ERROR: Could not create virtual environment.
        echo  Make sure Python is installed correctly.
        pause
        exit /b 1
    )
    echo  Virtual environment created successfully.
)
echo.

REM ============================================================
REM Step 3: Install all dependencies
REM ============================================================
echo  [Step 3/5] Installing all dependencies (this may take a few minutes)...
echo.

"%PIP%" install --upgrade pip >nul 2>&1
"%PIP%" install -r "%PROJECT_DIR%requirements.txt"
if errorlevel 1 (
    echo.
    echo  ERROR: Failed to install some dependencies.
    echo  Check your internet connection and try again.
    pause
    exit /b 1
)
echo.
echo  All dependencies installed successfully.
echo.

REM ============================================================
REM Step 4: Run tests to verify everything works
REM ============================================================
echo  [Step 4/5] Running tests to verify installation...
echo.

"%VENV_DIR%\Scripts\pytest.exe" "%PROJECT_DIR%tests" -v --tb=short
if errorlevel 1 (
    echo.
    echo  WARNING: Some tests did not pass.
    echo  The bot may still work - check the output above.
) else (
    echo.
    echo  All tests passed!
)
echo.

REM ============================================================
REM Step 5: Check .env file
REM ============================================================
echo  [Step 5/5] Checking configuration files...
echo.

if not exist "%PROJECT_DIR%.env" (
    echo  WARNING: No .env file found!
    echo.
    echo  The .env file contains your API keys and secrets.
    echo  Please make sure your .env file is in:
    echo     %PROJECT_DIR%.env
    echo.
    echo  You can copy .env.example and fill in your values:
    echo     copy "%PROJECT_DIR%.env.example" "%PROJECT_DIR%.env"
    echo.
) else (
    echo  .env file found - OK
)

if not exist "%PROJECT_DIR%config.toml" (
    echo  WARNING: No config.toml found!
) else (
    echo  config.toml found - OK
)

if not exist "%PROJECT_DIR%telegram_groups.toml" (
    echo  WARNING: No telegram_groups.toml found!
) else (
    echo  telegram_groups.toml found - OK
)

echo.
echo  ============================================================
echo       INSTALLATION COMPLETE!
echo  ============================================================
echo.
echo  What to do next:
echo.
echo  1. Make sure your .env file has the correct API keys
echo.
echo  2. Run SETUP_TELEGRAM.bat to connect your Telegram account
echo     (you only need to do this ONCE)
echo.
echo  3. Run START.bat to start the trading bot
echo.
echo  ============================================================
echo.
pause
