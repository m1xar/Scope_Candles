param(
    [string[]]$Names = @("c1", "c2"),
    [switch]$Force,
    [string]$Root
)

$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_root.ps1"
$Root = Resolve-MT5Root $Root

$masterExe = Get-MT5MasterExe $Root
if (-not (Test-Path $masterExe)) { throw "master not found at $masterExe" }
$master = Split-Path $masterExe -Parent
if (Get-Process -Name "terminal64" -ErrorAction SilentlyContinue) {
    throw "a terminal64.exe is still running - close every instance before cloning"
}

$transient = @("logs", "Bases", "bases", "Tester", "llm-agent", "MQL5\logs", "MQL5\Files\Temp")
$paths = @()

foreach ($name in $Names) {
    $dst = Join-Path $Root $name

    if (Test-Path $dst) {
        if (-not $Force) { throw "$dst already exists - pass -Force to overwrite" }
        Remove-Item $dst -Recurse -Force
    }

    robocopy $master $dst /E /NFL /NDL /NJH /NJS /NP | Out-Null
    if ($LASTEXITCODE -ge 8) { throw "robocopy failed for $dst (exit $LASTEXITCODE)" }

    foreach ($item in $transient) {
        $path = Join-Path $dst $item
        if (Test-Path $path) {
            Get-ChildItem $path -Recurse -Force | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    Remove-Item (Join-Path $dst "Config\dnsperf.dat") -Force -ErrorAction SilentlyContinue

    $paths += Join-Path $dst "terminal64.exe"
    $mb = [math]::Round(((Get-ChildItem $dst -Recurse -File | Measure-Object Length -Sum).Sum) / 1MB, 1)
    Write-Host ("  {0} -> {1}  ({2} MB)" -f $name, $dst, $mb)
}

Write-Host ""
Write-Host "Put these in .env :"
Write-Host ""
Write-Host ("MT5_CANDLES_CANDLES_TERMINAL_PATH=" + $paths[0])
if ($paths.Count -gt 1) {
    Write-Host ("MT5_CANDLES_PRICE_TERMINAL_PATH=" + $paths[1])
}
