@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"

set "EXE_PATH=%~dp0MailBot.exe"
set "CONFIG_FILE=%~dp0config.yaml"
set "CONFIG_EXAMPLE=%~dp0config.example.yaml"

if not exist "%EXE_PATH%" (
    echo ERROR: Letterbot executable (MailBot.exe) not found.
    exit /b 1
)

if not exist "%CONFIG_FILE%" (
    if exist "%CONFIG_EXAMPLE%" (
        copy /Y "%CONFIG_EXAMPLE%" "%CONFIG_FILE%" >nul
        echo =============================================
        echo   CONFIGURATION REQUIRED
        echo   Откройте config.yaml и заполните значения в кавычках.
        echo =============================================
        notepad "%CONFIG_FILE%"
    ) else (
        echo ERROR: config.example.yaml not found.
    )
    exit /b 1
)

"%EXE_PATH%"
exit /b %ERRORLEVEL%
