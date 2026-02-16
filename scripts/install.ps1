<#
Streamline install script for Windows.
Downloads the correct release binary and installs it locally.

Usage:
  .\scripts\install.ps1 [-Version <version>] [-InstallDir <dir>]

Environment variables:
  STREAMLINE_VERSION
  STREAMLINE_INSTALL_DIR
#>

param(
    [string]$Version = $env:STREAMLINE_VERSION,
    [string]$InstallDir = $env:STREAMLINE_INSTALL_DIR
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$Repo = "josedab/streamline"

if (-not $Version) {
    $Version = "latest"
}

if (-not $InstallDir) {
    $InstallDir = Join-Path $env:LOCALAPPDATA "Streamline\bin"
}

function Get-LatestVersion {
    $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
    $tag = $response.tag_name
    if (-not $tag) {
        throw "Unable to determine latest version."
    }
    if ($tag.StartsWith("v")) {
        return $tag.Substring(1)
    }
    return $tag
}

if ($Version -eq "latest") {
    $Version = Get-LatestVersion
}

$tag = "v$Version"

$arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
switch ($arch) {
    "AMD64" { $target = "x86_64-pc-windows-msvc" }
    default { throw "Unsupported architecture: $arch (supported: AMD64)" }
}

$asset = "streamline-$tag-$target.zip"
$url = "https://github.com/$Repo/releases/download/$tag/$asset"
$checksumUrl = "https://github.com/$Repo/releases/download/$tag/checksums.txt"

$tempDir = Join-Path $env:TEMP ([System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
    $zipPath = Join-Path $tempDir $asset
    Write-Host "Downloading $url"
    Invoke-WebRequest -Uri $url -OutFile $zipPath

    $checksumsPath = Join-Path $tempDir "checksums.txt"
    try {
        Invoke-WebRequest -Uri $checksumUrl -OutFile $checksumsPath
        $checksumLine = Get-Content $checksumsPath | Where-Object { $_ -match [regex]::Escape($asset) } | Select-Object -First 1
        if ($checksumLine) {
            $expected = $checksumLine.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)[0]
            $actual = (Get-FileHash -Algorithm SHA256 $zipPath).Hash.ToLowerInvariant()
            if ($expected.ToLowerInvariant() -ne $actual) {
                throw "Checksum verification failed."
            }
        }
    } catch {
        # Checksums are optional for the installer.
    }

    if (Test-Path $InstallDir) {
        Remove-Item -Recurse -Force (Join-Path $InstallDir "streamline.exe") -ErrorAction SilentlyContinue
        Remove-Item -Recurse -Force (Join-Path $InstallDir "streamline-cli.exe") -ErrorAction SilentlyContinue
    }

    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    Expand-Archive -Path $zipPath -DestinationPath $tempDir -Force

    Copy-Item (Join-Path $tempDir "streamline.exe") (Join-Path $InstallDir "streamline.exe") -Force
    Copy-Item (Join-Path $tempDir "streamline-cli.exe") (Join-Path $InstallDir "streamline-cli.exe") -Force
} finally {
    Remove-Item -Recurse -Force $tempDir
}

Write-Host "Installed Streamline to $InstallDir"
Write-Host "Add $InstallDir to your PATH if needed."
