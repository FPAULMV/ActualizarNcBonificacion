@echo off
cd /d "%~dp0"
setlocal enabledelayedexpansion
set "LOGFILE=..\storage\logs\registro_ejecuciones_bat.txt"
set "PS_SCRIPT=main.ps1"
set "START=%date% %time%"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"
set "EXITCODE=%ERRORLEVEL%"
set "END=%date% %time%"
if %EXITCODE%==0 (
    set "STATUS=OK"
    set "DETAIL=Finalizado con éxito."
) else (
    set "STATUS=ERROR %EXITCODE%"
    set "DETAIL=Hubo un error en la ejecución del script."
)
echo [%START%] -> [%END%] ^| %STATUS% ^| %DETAIL%>> "%LOGFILE%"
endlocal
exit
