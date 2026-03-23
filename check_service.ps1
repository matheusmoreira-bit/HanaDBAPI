# ...existing code...
param(
  [string] $TaskName = "",        # opcional: nome exato da tarefa, deixe vazio para procurar por start.ps1
  [int] $Port = 8000,             # porta do uvicorn
  [string] $HealthPath = "/"      # endpoint de verificação HTTP
)

Write-Output "== Verificação de tarefa agendada =="
if ($TaskName) {
  try {
    Get-ScheduledTask -TaskName $TaskName | Get-ScheduledTaskInfo
  } catch {
    Write-Output "Tarefa '$TaskName' não encontrada."
  }
} else {
  $tasks = Get-ScheduledTask |
    Where-Object { ($_.Actions | Out-String) -match 'start\.ps1' }
  if ($tasks) {
    $tasks | ForEach-Object { $_.TaskName; Get-ScheduledTaskInfo $_.TaskName }
  } else {
    Write-Output "Nenhuma tarefa que chama start.ps1 encontrada."
  }
}

Write-Output "`n== Processos relacionados (python / uvicorn / run_api.py / start.ps1) =="
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match 'python|uvicorn|run_api\.py|start\.ps1' } |
  Select-Object ProcessId,CommandLine |
  Format-Table -AutoSize

Write-Output "`n== Porta $Port (escutando?) =="
try {
  $listening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop
  if ($listening) {
    $listening | Select-Object LocalAddress,LocalPort,State
  }
} catch {
  Write-Output "Porta $Port não está em estado Listen."
}

Write-Output "`n== Probe HTTP em http://localhost:$Port$HealthPath =="
try {
  $resp = Invoke-WebRequest -Uri "http://localhost:$Port$HealthPath" -UseBasicParsing -TimeoutSec 5
  Write-Output "Status: $($resp.StatusCode) $($resp.StatusDescription)"
} catch {
  Write-Output "Falha HTTP: $($_.Exception.Message)"
}

# Pausa para evitar que a janela feche imediatamente
Read-Host -Prompt "Pressione Enter para sair"
# ...existing code...