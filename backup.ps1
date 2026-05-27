<# 
.SYNOPSIS
    StockAgent Project Backup Script

.DESCRIPTION
    Copy entire project to E:\AgentStudy as backup
    Excludes: venv, node_modules, __pycache__, .git, etc.

.PARAMETER Dest
    Destination directory (default: E:\AgentStudy\stockAgent_backup)

.EXAMPLE
    .\backup.ps1
    .\backup.ps1 -Dest "D:\backup"
#>

param(
    [string]$Dest = "E:\AgentStudy\stockAgent_backup"
)

# Source directory
$Source = $PSScriptRoot

# Timestamp
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupDir = "${Dest}_${Timestamp}"

Write-Host "============================================================"
Write-Host "StockAgent Project Backup"
Write-Host "============================================================"
Write-Host ""
Write-Host "Source:      $Source"
Write-Host "Destination: $BackupDir"
Write-Host ""

# Excluded directories
$ExcludeDirs = @(
    "venv",
    "env",
    ".venv",
    "node_modules",
    "__pycache__",
    ".git",
    ".idea",
    ".vscode",
    "dist",
    "build",
    ".pytest_cache",
    ".mypy_cache",
    "data"
)

# Excluded files
$ExcludeFiles = @(
    "*.pyc",
    "*.pyo",
    "*.log",
    ".env",
    ".env.local",
    "*.db"
)

Write-Host "Excluded directories:"
$ExcludeDirs | ForEach-Object { Write-Host "  - $_" }
Write-Host ""

# Create destination directory
if (-not (Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
    Write-Host "Created directory: $BackupDir"
}

# Build robocopy arguments
$RobocopyArgs = @(
    $Source,
    $BackupDir,
    "/E",
    "/NFL",
    "/NDL",
    "/NJH",
    "/NJS",
    "/NC",
    "/NS",
    "/NP"
)

# Add excluded directories
foreach ($dir in $ExcludeDirs) {
    $RobocopyArgs += "/XD"
    $RobocopyArgs += $dir
}

# Add excluded files
foreach ($file in $ExcludeFiles) {
    $RobocopyArgs += "/XF"
    $RobocopyArgs += $file
}

Write-Host "Copying files..."
$startTime = Get-Date

# Execute robocopy
& robocopy @RobocopyArgs | Out-Null

$endTime = Get-Date
$duration = [math]::Round(($endTime - $startTime).TotalSeconds, 2)

# Statistics
$fileCount = (Get-ChildItem -Path $BackupDir -Recurse -File -ErrorAction SilentlyContinue).Count
$dirCount = (Get-ChildItem -Path $BackupDir -Recurse -Directory -ErrorAction SilentlyContinue).Count
$totalSize = (Get-ChildItem -Path $BackupDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
$sizeMB = [math]::Round($totalSize / 1MB, 2)

Write-Host ""
Write-Host "============================================================"
Write-Host "Backup Complete!"
Write-Host "============================================================"
Write-Host ""
Write-Host "Statistics:"
Write-Host "  Directories: $dirCount"
Write-Host "  Files:       $fileCount"
Write-Host "  Total Size:  $sizeMB MB"
Write-Host "  Duration:    $duration seconds"
Write-Host ""
Write-Host "Backup Location: $BackupDir"
Write-Host ""
