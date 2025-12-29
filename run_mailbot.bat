@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1

rem Move to repo root
cd /d "%~dp0"
set REPO_ROOT=%CD%

echo =============================================
echo   MailBot Premium v26 - Run
echo =============================================

echo Checking repository status...
if exist .git (
    git --version >nul 2>&1
    if %ERRORLEVEL%==0 (
        for /f "usebackq delims=" %%I in (`git rev-parse --abbrev-ref HEAD 2^>nul`) do set GIT_BRANCH=%%I
        if defined GIT_BRANCH echo Git branch: %GIT_BRANCH%
    ) else (
        echo Git repository detected but git is not available.
    )
) else (
    echo Warning: .git not found, continuing without git metadata.
)

echo Checking virtual environment...
if not exist "%REPO_ROOT%\venv\Scripts\activate.bat" (
    echo ERROR: venv not found. Please run install_and_run.bat first.
    pause
    exit /b 1
)

call "%REPO_ROOT%\venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    pause
    exit /b 1
)

echo Starting MailBot...
python -m mailbot_v26
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: MailBot terminated with errors.
) else (
    echo MailBot finished.
)

echo =============================================
echo   DONE. Close this window or press a key.
echo =============================================
pause
endlocal
