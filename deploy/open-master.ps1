param([string]$Root)

$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_root.ps1"
$Root = Resolve-MT5Root $Root

$masterExe = Get-MT5MasterExe $Root
if (-not (Test-Path $masterExe)) { throw "master not found at $masterExe" }
if (Get-Process -Name "terminal64" -ErrorAction SilentlyContinue) {
    throw "a terminal64.exe is already running - close it first"
}

Start-Process -FilePath $masterExe -ArgumentList "/portable"
Write-Host "Opened $masterExe"
Write-Host "Add the broker under File > Open an Account, then close the terminal so Config\servers.dat is written."
