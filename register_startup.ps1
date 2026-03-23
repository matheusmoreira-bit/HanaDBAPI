# Dicas rápidas:
# - Iniciar manualmente (janela para acompanhar): 
#     powershell -NoProfile -ExecutionPolicy Bypass -File ".\start.ps1"
# - Iniciar em segundo plano (fechar janela): 
#     Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','".\start.ps1"' -WindowStyle Hidden
# - Registrar tarefa para iniciar no boot (execute este script como Administrador):
#     .\register_startup.ps1
# - Registrar e iniciar agora (executa start.ps1 numa nova janela que permanece aberta):
#     .\register_startup.ps1 -RunNow

param(
    [string]$TaskName = "HanaDBAPI",
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TargetScript = Join-Path $ScriptDir "start.ps1"

$currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    throw "Execute este script como Administrador para registrar a tarefa agendada de inicializacao."
}

$powershellExe = (Get-Command powershell.exe -ErrorAction Stop).Source
$escapedScript = $TargetScript.Replace('"', '""')
$actionArgs = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$escapedScript`""

$action = New-ScheduledTaskAction -Execute $powershellExe -Argument $actionArgs
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Inicializa e supervisiona a HanaDBAPI no boot do Windows." -Force | Out-Null
Write-Host "Tarefa agendada '$TaskName' criada/atualizada para iniciar com o Windows."

# Se solicitado, iniciar start.ps1 agora em nova janela e manter aberta para acompanhar requisições/logs
if ($RunNow) {
    Write-Host "Iniciando start.ps1 agora em nova janela (manter aberta)..."
    Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -NoExit -File `"$TargetScript`"" -WorkingDirectory $ScriptDir
}
