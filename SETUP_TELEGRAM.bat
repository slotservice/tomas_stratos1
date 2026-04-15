@echo off
chcp 65001 >nul 2>&1
title Stratos1 - Telegram Account Setup
color 0E

set "PROJECT_DIR=%~dp0"
set "VENV_DIR=%PROJECT_DIR%venv"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"

echo.
echo  ============================================================
echo       STRATOS1 - TELEGRAM ACCOUNT SETUP
echo  ============================================================
echo.
echo  This connects your Telegram account to the bot.
echo  You only need to do this ONCE.
echo.
echo  You will be asked for:
echo    1. Your phone number (with country code, e.g. +46...)
echo    2. The verification code Telegram sends you
echo    3. Your 2FA password (if you have one)
echo.
echo  After this, a session key will be shown.
echo  You must copy it into your .env file.
echo.
echo  ============================================================
echo.
pause

REM Check venv exists
if not exist "%PYTHON%" (
    echo  ERROR: Virtual environment not found!
    echo  Run INSTALL.bat first.
    echo.
    pause
    exit /b 1
)

REM Check .env exists
if not exist "%PROJECT_DIR%.env" (
    echo  ERROR: .env file not found!
    echo  Create your .env file first with TG_API_ID and TG_API_HASH.
    echo.
    pause
    exit /b 1
)

echo.
echo  Starting Telegram session generator...
echo.

"%PYTHON%" -X utf8 "%PROJECT_DIR%generate_session.py"

echo.
echo  ============================================================
echo.
echo  If it worked, copy the TG_SESSION_STRING line above
echo  and paste it into your .env file.
echo.
echo  Then run START.bat to start the bot.
echo.
echo  ============================================================
echo.
pause
