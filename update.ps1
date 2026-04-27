$ErrorActionPreference = "Stop"

$bundleRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("vtuber-update-" + [guid]::NewGuid().ToString("N"))
$zipPath = Join-Path $tempRoot "VTuber-main.zip"
$extractDir = Join-Path $tempRoot "extract"
$zipUrl = "https://github.com/vuhv2909/VTuber/archive/refs/heads/main.zip"

function Test-SkipPath {
    param(
        [string]$RelativePath
    )

    $normalized = $RelativePath.Replace("/", "\")
    if ($normalized -like "reup_outputs\*") { return $true }
    if ($normalized -like "yt_reup_tool\runtime\logs\*") { return $true }
    if ($normalized -like "yt_reup_tool\runtime\state*.json") { return $true }
    return $false
}

try {
    Write-Host "Downloading latest TV Media bundle..."
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null
    Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath

    Write-Host "Extracting update package..."
    Expand-Archive -LiteralPath $zipPath -DestinationPath $extractDir -Force

    $sourceRoot = Get-ChildItem -LiteralPath $extractDir -Directory | Select-Object -First 1
    if (-not $sourceRoot) {
        throw "Could not find extracted bundle root."
    }

    Write-Host "Applying update..."
    Get-ChildItem -LiteralPath $sourceRoot.FullName -Recurse -File | ForEach-Object {
        $relativePath = $_.FullName.Substring($sourceRoot.FullName.Length).TrimStart("\", "/")
        if (Test-SkipPath -RelativePath $relativePath) {
            return
        }

        $targetPath = Join-Path $bundleRoot $relativePath
        $targetDir = Split-Path -Parent $targetPath
        if ($targetDir) {
            New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
        }
        Copy-Item -LiteralPath $_.FullName -Destination $targetPath -Force
    }

    Write-Host "Update package applied."
}
finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}
