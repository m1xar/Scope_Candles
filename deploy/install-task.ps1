param(
    [string]$TaskName = "ScopeCandles",
    [string]$User = "SYSTEM"
)

$ErrorActionPreference = "Stop"

$project = Split-Path $PSScriptRoot -Parent
$script = Join-Path $PSScriptRoot "run.ps1"

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$script`"" `
    -WorkingDirectory $project

$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -User $User -RunLevel Highest | Out-Null

Write-Host "Registered scheduled task '$TaskName'."
Write-Host "Start it now with:  Start-ScheduledTask -TaskName $TaskName"
