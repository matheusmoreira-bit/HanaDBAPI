param(
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

function Start-Runner {
    param([string]$PythonCommand)

    Write-Host "Iniciando supervisor da API com $PythonCommand"
    try {
        & $PythonCommand $RunnerPath
        $exitCode = $LASTEXITCODE
        Write-Host "Processo do runner terminou com codigo $exitCode"
    } catch {
        Write-Host "Runner terminou com erro: $_"
    }

    Write-Host ""
    Read-Host "Pressione Enter para fechar esta janela e encerrar o start.ps1"
}

$pythonCommand = Resolve-Python -RequestedPython $PythonExe

# Ao executar diretamente este script, ele inicia o servidor e mantém a janela aberta.
Start-Runner -PythonCommand $pythonCommand
