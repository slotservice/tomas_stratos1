@echo off
chcp 65001 >nul 2>&1
title Stratos1 Trading Bot - Running
color 0B

set "PROJECT_DIR=%~dp0"
set "VENV_DIR=%PROJECT_DIR%venv"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"

echo.
echo  ============================================================
echo       STRATOS1 TRADING BOT - STARTING
echo  ============================================================
echo.

REM ============================================================
REM Check 1: Virtual environment exists
REM ============================================================
if not exist "%PYTHON%" (
    echo  ERROR: Virtual environment not found!
    echo.
    echo  Run INSTALL.bat first to set up the environment.
    echo.
    pause
    exit /b 1
)

REM ============================================================
REM Check 2: .env file exists
REM ============================================================
if not exist "%PROJECT_DIR%.env" (
    echo  ERROR: .env file not found!
    echo.
    echo  Your .env file must contain your API keys.
    echo  Copy .env.example to .env and fill in your values.
    echo.
    pause
    exit /b 1
)

REM ============================================================
REM Check 3: config.toml exists
REM ============================================================
if not exist "%PROJECT_DIR%config.toml" (
    echo  ERROR: config.toml not found!
    echo.
    echo  The config file contains all trading parameters.
    echo  It should be in the same folder as this file.
    echo.
    pause
    exit /b 1
)

REM ============================================================
REM Quick validation
REM ============================================================
echo  Checking configuration...
echo.

"%PYTHON%" -X utf8 -c "from config.settings import load_settings; s = load_settings(); print(f'  Wallet: {s.wallet.bot_wallet} USDT'); print(f'  Risk: {s.wallet.risk_pct*100}%%'); print(f'  Bybit Demo: {s.bybit.demo}'); print(f'  Groups loaded: {len(s.telegram_groups)}'); print(f'  Timezone: {s.general.timezone}'); print(); print('  Configuration OK!')"
if errorlevel 1 (
    echo.
    echo  ERROR: Configuration check failed!
    echo  Check your .env and config.toml files.
    echo.
    pause
    exit /b 1
)

echo.
echo  ============================================================
echo  Starting the bot now...
echo  Press Ctrl+C to stop the bot at any time.
echo  ============================================================
echo.

REM ============================================================
REM Start the bot
REM ============================================================
"%PYTHON%" -X utf8 "%PROJECT_DIR%main.py"

REM If we get here, the bot stopped
echo.
echo  ============================================================
echo  Bot has stopped.
echo  ============================================================
echo.
echo  If the bot crashed, check the log file:
echo     %PROJECT_DIR%stratos1.log
echo.
pause
