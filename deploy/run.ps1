param([string]$Root = $null)

$ErrorActionPreference = "Stop"

$project = Split-Path $PSScriptRoot -Parent
Set-Location $project

$python = Join-Path $project ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }

& $python -X utf8 "$project\main.py"
