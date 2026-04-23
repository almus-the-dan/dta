#
# Profile the DTA reader and writer with samply, then report the
# top functions from this crate by inclusive/self-time percentage.
#
# Usage:
#   .\profile.ps1 [-Records N] [-Top N]
#   .\profile.ps1                        # 1M records, top 10 functions
#   .\profile.ps1 -Records 500000        # 500K records, top 10 functions
#   .\profile.ps1 -Top 20                # 1M records, top 20 functions
#
# Requirements:
#   cargo install --locked samply
#
#   On Windows, samply uses ETW and must run from an Administrator
#   PowerShell window. Launch "Windows Terminal" or "PowerShell" with
#   "Run as administrator" before invoking this script.

param(
    [int]$Records = 1000000,
    [int]$Top = 10
)

$ErrorActionPreference = "Stop"

$ProfileBin = "target/profiling/examples/profile.exe"
$ReportBin  = "target/profiling/examples/profile_report.exe"
$DataFile   = "target/profile_bench.dta"

# --- Preflight ---

if (-not (Get-Command samply -ErrorAction SilentlyContinue)) {
    Write-Error "Error: 'samply' not found. Install with: cargo install --locked samply"
    exit 1
}

# Warn (not hard-fail) if not elevated; samply will still print a clear
# error if it actually can't start an ETW session.
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object System.Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Warning "Not running as Administrator. samply uses ETW and normally needs elevation; if recording fails with an access-denied error, relaunch from an elevated shell."
    Write-Host ""
}

Write-Host "Building profiling binaries..."
cargo build --example profile --example profile_report --profile profiling -p dta --all-features --quiet
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# --- Profile each phase ---

function Run-Phase {
    param([string]$Phase)

    $JsonFile = "target/profile_${Phase}.json.gz"
    $SymsFile = $JsonFile -replace '\.gz$', '.syms.json'

    Write-Host ""
    Write-Host "Recording $Phase phase ($Records records)..."

    # Intentionally do NOT redirect samply's stderr: its permission /
    # ETW errors are the most useful diagnostic when recording fails.
    samply record --save-only --unstable-presymbolicate -o $JsonFile `
        -- "./$ProfileBin" --phase $Phase --records $Records --file $DataFile
    $SamplyExit = $LASTEXITCODE

    if ($SamplyExit -ne 0 -or -not (Test-Path $JsonFile)) {
        Write-Warning "samply did not produce '$JsonFile' for $Phase phase (exit code $SamplyExit). Skipping report."
        Remove-Item -Force $JsonFile, $SymsFile -ErrorAction SilentlyContinue
        return
    }

    Write-Host ""
    Write-Host "=== $($Phase.ToUpper()) ==="
    & "./$ReportBin" --input $JsonFile --top $Top

    Remove-Item -Force $JsonFile, $SymsFile -ErrorAction SilentlyContinue
}

Run-Phase "write"
Run-Phase "read"
Run-Phase "async-write"
Run-Phase "async-read"

Remove-Item -Force $DataFile -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "Done."
