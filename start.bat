@echo off
cd /d "%~dp0"
echo Starting Humsync server...
start "Humsync" http://localhost:8000
python server.py
pause