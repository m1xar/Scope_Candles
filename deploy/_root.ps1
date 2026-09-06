function Resolve-MT5Root {
    param([string]$Root)

    if ($Root) { return $Root.TrimEnd('\') }

    $envFile = Join-Path (Split-Path $PSScriptRoot -Parent) ".env"
    if (Test-Path $envFile) {
        $line = Select-String -Path $envFile -Pattern '^MT5_CANDLES_CANDLES_TERMINAL_PATH=' | Select-Object -First 1
        if ($line) {
            $first = ($line.Line -replace '^MT5_CANDLES_CANDLES_TERMINAL_PATH=', '').Trim()
            if ($first) { return (Split-Path (Split-Path $first -Parent) -Parent) }
        }
    }

    return "C:\MT5"
}

function Get-MT5MasterExe {
    param([string]$Root)
    return "$($Root.TrimEnd('\'))\master\terminal64.exe"
}
