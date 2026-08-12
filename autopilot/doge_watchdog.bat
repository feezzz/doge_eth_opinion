@echo off
cd /d "%~dp0"
echo [%date% %time%] doge watchdog started

:loop
python -X utf8 doge_watchdog.py >> watchdog.log 2>&1
echo [%date% %time%] watchdog restarted
timeout /t 10 /nobreak >nul
goto loop
