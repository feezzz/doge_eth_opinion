@echo off
cd /d D:\code\doge\doge_eth_opinion\dashboard
python build_dashboard.py
echo.
echo === 刷新完成，在浏览器中打开 index.html ===
start "" index.html
