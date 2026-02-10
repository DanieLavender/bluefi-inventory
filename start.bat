@echo off
chcp 65001 >nul
title 블루파이 재고관리
echo.
echo  ========================================
echo    블루파이 재고관리 서버 시작
echo  ========================================
echo.
echo  브라우저에서 접속: http://localhost:3000
echo  종료: 이 창을 닫거나 Ctrl+C
echo.
echo  ----------------------------------------
echo.
start http://localhost:3000
node server.js
pause
