# Paths
$VenvPath = "..\.venv\Scripts"
$ScriptPath = "..\..\src\"
$ScriptName = "main.py"

# Archivo final de log
$logPath = "..\storage\logs\registro_ejecuciones_ps1.txt"

try {
    # Activar entorno
    Set-Location $VenvPath
    .\activate

    # Ir al script
    Set-Location $ScriptPath

    # Ejecutar Python VERBATIM -> imprime todo en consola
    python.exe ".\$ScriptName"

    # Desactivar venv
    deactivate

    # Registrar solo el final
    $endStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logPath -Value "Finalizado correctamente a las $endStamp"

    exit 0
}
catch {
    $errStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logPath -Value "Error a las $errStamp : $($_.Exception.Message)"
    exit 1
}
