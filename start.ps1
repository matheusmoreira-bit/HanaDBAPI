param(
    [switch]$RunLoop,
    [string]$TaskName = "HanaDBAPI",
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunnerPath = Join-Path $ScriptDir "run_api.py"

function Resolve-Python {
    param([string]$RequestedPython)

    $candidates = @()
    if ($RequestedPython) {
        $candidates += $RequestedPython
    }
    $candidates += @("python", "py")

    foreach ($candidate in $candidates) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            return $command.Source
        }
    }

    throw "Nao foi possivel localizar um interpretador Python. Use -PythonExe para informar um caminho."
}

function Install-StartupTask {
    param([string]$CurrentTaskName)

    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
    $isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

    if (-not $isAdmin) {
        throw "Execute este script como Administrador para registrar a tarefa agendada de inicializacao."
    }

    $powershellExe = (Get-Command powershell.exe -ErrorAction Stop).Source
    $escapedScript = $PSCommandPath.Replace('"', '""')
    $actionArgs = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$escapedScript`" -RunLoop"

    $action = New-ScheduledTaskAction -Execute $powershellExe -Argument $actionArgs
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)

    Register-ScheduledTask -TaskName $CurrentTaskName -Action $action -Trigger $trigger -Settings $settings -Description "Inicializa e supervisiona a HanaDBAPI no boot do Windows." -Force | Out-Null
    Write-Host "Tarefa agendada '$CurrentTaskName' criada/atualizada para iniciar com o Windows."
}

function Start-Runner {
    param([string]$PythonCommand)

    Write-Host "Iniciando supervisor da API com $PythonCommand"
    & $PythonCommand $RunnerPath
    exit $LASTEXITCODE
}

$pythonCommand = Resolve-Python -RequestedPython $PythonExe

if (-not $RunLoop) {
    Install-StartupTask -CurrentTaskName $TaskName
}

Start-Runner -PythonCommand $pythonCommand
