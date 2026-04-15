@echo off
REM ============================================================
REM Stratos1 Trading Bot - Windows Build Script
REM ============================================================
REM Creates a virtual environment, installs dependencies, runs
REM tests, and builds a standalone Windows executable.
REM
REM Usage:
REM   build.bat            - Full build (venv + install + test + exe)
REM   build.bat test       - Run tests only
REM   build.bat exe        - Build exe only (assumes venv exists)
REM ============================================================

setlocal enabledelayedexpansion

set "PROJECT_DIR=%~dp0"
set "VENV_DIR=%PROJECT_DIR%venv"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"
set "PIP=%VENV_DIR%\Scripts\pip.exe"
set "PYTEST=%VENV_DIR%\Scripts\pytest.exe"
set "PYINSTALLER=%VENV_DIR%\Scripts\pyinstaller.exe"

REM Handle command-line arguments
if "%1"=="test" goto :run_tests
if "%1"=="exe" goto :build_exe

REM ============================================================
REM Step 1: Create virtual environment
REM ============================================================
echo.
echo [1/4] Creating virtual environment...
if not exist "%VENV_DIR%" (
    python -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo ERROR: Failed to create virtual environment.
        echo Make sure Python 3.11+ is installed and on your PATH.
        exit /b 1
    )
    echo Virtual environment created at %VENV_DIR%
) else (
    echo Virtual environment already exists, skipping creation.
)

REM ============================================================
REM Step 2: Install dependencies
REM ============================================================
echo.
echo [2/4] Installing dependencies...
"%PIP%" install --upgrade pip
"%PIP%" install -r "%PROJECT_DIR%requirements.txt"
if errorlevel 1 (
    echo ERROR: Failed to install dependencies.
    exit /b 1
)
echo Dependencies installed successfully.

REM ============================================================
REM Step 3: Run tests
REM ============================================================
:run_tests
echo.
echo [3/4] Running tests...
"%PYTEST%" "%PROJECT_DIR%tests" -v --tb=short
if errorlevel 1 (
    echo.
    echo WARNING: Some tests failed. Review output above.
    echo Continuing with build anyway...
) else (
    echo All tests passed.
)

if "%1"=="test" goto :done

REM ============================================================
REM Step 4: Build executable with PyInstaller
REM ============================================================
:build_exe
echo.
echo [4/4] Building executable with PyInstaller...
"%PYINSTALLER%" ^
    --onefile ^
    --name stratos1 ^
    --add-data "%PROJECT_DIR%config.toml;." ^
    --add-data "%PROJECT_DIR%telegram_groups.toml;." ^
    --hidden-import=structlog ^
    --hidden-import=pydantic ^
    --hidden-import=pydantic_settings ^
    --hidden-import=aiosqlite ^
    --hidden-import=telethon ^
    --hidden-import=pybit ^
    --hidden-import=apscheduler ^
    --noconfirm ^
    --clean ^
    "%PROJECT_DIR%main.py"

if errorlevel 1 (
    echo ERROR: PyInstaller build failed.
    exit /b 1
)

echo.
echo ============================================================
echo Build complete!
echo Executable: %PROJECT_DIR%dist\stratos1.exe
echo ============================================================

:done
endlocal
