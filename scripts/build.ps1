# Build wrapper: initializes MSVC environment (vcvars64) then runs cargo.
# Usage: powershell -File scripts\build.ps1 [cargo args...]
$ErrorActionPreference = "Stop"
$vcvars = "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
if (-not (Test-Path $vcvars)) { throw "vcvars64.bat not found at $vcvars" }

# Capture the environment produced by vcvars64.bat and apply it to this process.
$envBlock = cmd /c "`"$vcvars`" >nul 2>&1 && set"
foreach ($line in $envBlock) {
    $i = $line.IndexOf('=')
    if ($i -gt 0) { [Environment]::SetEnvironmentVariable($line.Substring(0, $i), $line.Substring($i + 1), "Process") }
}

$env:PATH = "C:\Strawberry\perl\bin;C:\Strawberry\c\bin;$env:USERPROFILE\.cargo\bin;$env:PATH"
Set-Location $PSScriptRoot\..
& cargo @args
exit $LASTEXITCODE
