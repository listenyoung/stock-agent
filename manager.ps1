# StockAgent Service Manager
# Usage: .\manager.ps1

# ==================== Config ====================

$global:RootPath = $PSScriptRoot
$global:AgentPath = Join-Path $RootPath "AgentServer"
$global:FrontendPath = Join-Path $RootPath "frontend"
$global:LogPath = Join-Path $AgentPath "logs"
$global:VenvPath = Join-Path $AgentPath "venv\Scripts\activate.ps1"

# Process storage
$global:NodeProcesses = @{}

# Node config
$global:NodeConfig = @(
    @{Key="1"; Type="web"; Name="Web"; Desc="API Gateway"},
    @{Key="2"; Type="inference"; Name="Inference"; Desc="LLM Analysis"},
    @{Key="3"; Type="data_sync"; Name="DataSync"; Desc="Tushare Collector"},
    @{Key="4"; Type="listener"; Name="Listener"; Desc="Strategy Alert"},
    @{Key="5"; Type="backtest"; Name="Backtest"; Desc="Quant Backtest"}
)

# ==================== Utility Functions ====================

function Write-ColorText {
    param(
        [string]$Text,
        [string]$Color = "White"
    )
    Write-Host $Text -ForegroundColor $Color
}

function Write-Header {
    param([string]$Title)
    Write-Host ""
    Write-ColorText "========================================" "Cyan"
    Write-ColorText "  $Title" "Yellow"
    Write-ColorText "========================================" "Cyan"
    Write-Host ""
}

function Write-MenuItem {
    param(
        [string]$Key,
        [string]$Description,
        [string]$Status = ""
    )
    $keyText = "[$Key]"
    Write-Host "  $keyText " -ForegroundColor Green -NoNewline
    Write-Host $Description -NoNewline
    if ($Status) {
        $statusColor = if ($Status -eq "Running") { "Green" } elseif ($Status -eq "Stopped") { "Gray" } else { "Yellow" }
        Write-Host " ($Status)" -ForegroundColor $statusColor
    } else {
        Write-Host ""
    }
}

function Get-NodeStatus {
    param([string]$NodeType)
    
    if ($global:NodeProcesses.ContainsKey($NodeType)) {
        $proc = $global:NodeProcesses[$NodeType]
        if ($proc -and !$proc.HasExited) {
            return "Running"
        }
    }
    return "Stopped"
}

# ==================== Node Management ====================

function Start-AgentNode {
    param([string]$NodeType)
    
    if ((Get-NodeStatus $NodeType) -eq "Running") {
        Write-ColorText "  [!] $NodeType node is already running" "Yellow"
        return
    }
    
    if (!(Test-Path $global:VenvPath)) {
        Write-ColorText "  [X] Venv not found: $global:VenvPath" "Red"
        return
    }
    
    Write-ColorText "  [>] Starting $NodeType node..." "Cyan"
    
    $command = "`$Host.UI.RawUI.WindowTitle = 'StockAgent - $NodeType'; Set-Location '$global:AgentPath'; & '$global:VenvPath'; `$env:NODE_TYPE = '$NodeType'; python main.py"
    
    $proc = Start-Process powershell -ArgumentList @("-NoExit", "-Command", $command) -PassThru
    $global:NodeProcesses[$NodeType] = $proc
    
    Start-Sleep -Milliseconds 500
    Write-ColorText "  [OK] $NodeType node started (PID: $($proc.Id))" "Green"
}

function Stop-AgentNode {
    param([string]$NodeType)
    
    if ((Get-NodeStatus $NodeType) -ne "Running") {
        Write-ColorText "  [!] $NodeType node is not running" "Yellow"
        return
    }
    
    Write-ColorText "  [>] Stopping $NodeType node..." "Cyan"
    
    $proc = $global:NodeProcesses[$NodeType]
    if ($proc -and !$proc.HasExited) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        $global:NodeProcesses.Remove($NodeType)
    }
    
    Write-ColorText "  [OK] $NodeType node stopped" "Green"
}

function Start-Frontend {
    if ((Get-NodeStatus "frontend") -eq "Running") {
        Write-ColorText "  [!] Frontend is already running" "Yellow"
        return
    }
    
    if (!(Test-Path $global:FrontendPath)) {
        Write-ColorText "  [X] Frontend dir not found: $global:FrontendPath" "Red"
        return
    }
    
    Write-ColorText "  [>] Starting frontend dev server..." "Cyan"
    
    $command = "`$Host.UI.RawUI.WindowTitle = 'StockAgent - Frontend'; Set-Location '$global:FrontendPath'; npm run dev"
    $proc = Start-Process powershell -ArgumentList @("-NoExit", "-Command", $command) -PassThru
    $global:NodeProcesses["frontend"] = $proc
    
    Start-Sleep -Milliseconds 500
    Write-ColorText "  [OK] Frontend started (PID: $($proc.Id))" "Green"
    Write-ColorText "  -> Visit: http://localhost:5173" "Cyan"
}

function Stop-Frontend {
    if ((Get-NodeStatus "frontend") -ne "Running") {
        Write-ColorText "  [!] Frontend is not running" "Yellow"
        return
    }
    
    Write-ColorText "  [>] Stopping frontend..." "Cyan"
    
    $proc = $global:NodeProcesses["frontend"]
    if ($proc -and !$proc.HasExited) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        $global:NodeProcesses.Remove("frontend")
    }
    
    Write-ColorText "  [OK] Frontend stopped" "Green"
}

# ==================== Log Viewer ====================

function Show-LogMenu {
    while ($true) {
        Clear-Host
        Write-Header "View Logs"
        
        if (!(Test-Path $global:LogPath)) {
            New-Item -ItemType Directory -Path $global:LogPath -Force | Out-Null
        }
        
        Write-ColorText "  Log dir: $global:LogPath" "Gray"
        Write-Host ""
        
        $logFiles = @()
        $i = 1
        
        foreach ($node in $global:NodeConfig) {
            $logFile = Join-Path $global:LogPath "$($node.Type).log"
            $exists = Test-Path $logFile
            
            if ($exists) {
                $fileInfo = Get-Item $logFile
                $size = "{0:N1} KB" -f ($fileInfo.Length / 1KB)
                $modified = $fileInfo.LastWriteTime.ToString("MM-dd HH:mm:ss")
                Write-MenuItem $i "$($node.Name) log [$size, $modified]"
                $logFiles += @{Index=$i; Path=$logFile; Name=$node.Name}
            } else {
                Write-Host "  [$i] " -ForegroundColor DarkGray -NoNewline
                Write-Host "$($node.Name) log (empty)" -ForegroundColor DarkGray
            }
            $i++
        }
        
        Write-Host ""
        Write-Host "  -------------------------------" -ForegroundColor DarkGray
        Write-MenuItem "T" "Tail log (real-time)"
        Write-MenuItem "O" "Open in Notepad"
        Write-MenuItem "C" "Clear all logs"
        Write-Host ""
        Write-MenuItem "0" "Back to main menu"
        Write-Host ""
        
        $choice = Read-Host "Select"
        
        switch ($choice.ToUpper()) {
            "0" { return }
            "T" { Show-TailLogMenu $logFiles }
            "O" { Open-LogInNotepad $logFiles }
            "C" { Clear-AllLogs }
            default {
                $index = 0
                if ([int]::TryParse($choice, [ref]$index)) {
                    $selected = $logFiles | Where-Object { $_.Index -eq $index }
                    if ($selected) {
                        Show-LogContent $selected.Path $selected.Name
                    }
                }
            }
        }
    }
}

function Show-TailLogMenu {
    param($LogFiles)
    
    Write-Host ""
    Write-ColorText "  Select log to tail:" "Cyan"
    
    foreach ($log in $LogFiles) {
        Write-Host "    [$($log.Index)] $($log.Name)" -ForegroundColor Green
    }
    Write-Host ""
    
    $choice = Read-Host "Select"
    $index = 0
    if ([int]::TryParse($choice, [ref]$index)) {
        $selected = $LogFiles | Where-Object { $_.Index -eq $index }
        if ($selected -and (Test-Path $selected.Path)) {
            Write-Host ""
            Write-ColorText "  Tailing: $($selected.Name) log" "Yellow"
            Write-ColorText "  Press Ctrl+C to exit" "Gray"
            Write-Host ""
            
            try {
                Get-Content -Path $selected.Path -Tail 30 -Wait
            } catch {
                # User pressed Ctrl+C
            }
        }
    }
}

function Open-LogInNotepad {
    param($LogFiles)
    
    Write-Host ""
    Write-ColorText "  Select log to open:" "Cyan"
    
    foreach ($log in $LogFiles) {
        Write-Host "    [$($log.Index)] $($log.Name)" -ForegroundColor Green
    }
    Write-Host ""
    
    $choice = Read-Host "Select"
    $index = 0
    if ([int]::TryParse($choice, [ref]$index)) {
        $selected = $LogFiles | Where-Object { $_.Index -eq $index }
        if ($selected -and (Test-Path $selected.Path)) {
            Start-Process notepad $selected.Path
            Write-ColorText "  [OK] Opened in Notepad" "Green"
            Start-Sleep -Seconds 1
        }
    }
}

function Show-LogContent {
    param([string]$LogFile, [string]$Name)
    
    Clear-Host
    Write-Header "$Name Log (last 50 lines)"
    
    if (Test-Path $LogFile) {
        Get-Content -Path $LogFile -Tail 50 | ForEach-Object {
            $line = $_
            if ($line -match 'ERROR') {
                Write-Host $line -ForegroundColor Red
            } elseif ($line -match 'WARNING') {
                Write-Host $line -ForegroundColor Yellow
            } elseif ($line -match 'DEBUG') {
                Write-Host $line -ForegroundColor DarkGray
            } else {
                Write-Host $line
            }
        }
    } else {
        Write-ColorText "  Log file not found" "Gray"
    }
    
    Write-Host ""
    Write-Host "Press any key to return..." -ForegroundColor Gray
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

function Clear-AllLogs {
    Write-Host ""
    $confirm = Read-Host "Clear all logs? (y/N)"
    
    if ($confirm -eq "y" -or $confirm -eq "Y") {
        Get-ChildItem -Path $global:LogPath -Filter "*.log" -ErrorAction SilentlyContinue | ForEach-Object {
            Clear-Content $_.FullName -ErrorAction SilentlyContinue
        }
        Write-ColorText "  [OK] All logs cleared" "Green"
    } else {
        Write-ColorText "  Cancelled" "Gray"
    }
    
    Start-Sleep -Seconds 1
}

# ==================== Batch Operations ====================

function Start-AllNodes {
    Write-Header "Starting all backend nodes"
    
    foreach ($node in $global:NodeConfig) {
        Start-AgentNode $node.Type
        Start-Sleep -Seconds 1
    }
    
    Write-Host ""
    Write-ColorText "  All backend nodes started!" "Green"
}

function Stop-AllServices {
    Write-Header "Stopping all services"
    
    foreach ($key in @($global:NodeProcesses.Keys)) {
        if ((Get-NodeStatus $key) -eq "Running") {
            if ($key -eq "frontend") {
                Stop-Frontend
            } else {
                Stop-AgentNode $key
            }
        }
    }
    
    Write-Host ""
    Write-ColorText "  All services stopped!" "Green"
}

# ==================== Stop Menu ====================

function Show-StopMenu {
    Write-Header "Stop Service"
    
    foreach ($node in $global:NodeConfig) {
        Write-MenuItem $node.Key "Stop $($node.Name)" (Get-NodeStatus $node.Type)
    }
    Write-MenuItem "6" "Stop Frontend" (Get-NodeStatus "frontend")
    Write-Host ""
    Write-MenuItem "0" "Back to main menu"
    Write-Host ""
    
    $choice = Read-Host "Select service to stop"
    
    switch ($choice) {
        "1" { Stop-AgentNode "web" }
        "2" { Stop-AgentNode "inference" }
        "3" { Stop-AgentNode "data_sync" }
        "4" { Stop-AgentNode "listener" }
        "5" { Stop-AgentNode "backtest" }
        "6" { Stop-Frontend }
        "0" { return }
    }
    
    Start-Sleep -Seconds 1
}

# ==================== Main Menu ====================

function Show-MainMenu {
    Clear-Host
    Write-Header "StockAgent Service Manager"
    
    Write-ColorText "  Service Status:" "White"
    Write-Host "  -------------------------------"
    
    foreach ($node in $global:NodeConfig) {
        $status = Get-NodeStatus $node.Type
        $statusColor = if ($status -eq "Running") { "Green" } else { "DarkGray" }
        $statusIcon = if ($status -eq "Running") { "[*]" } else { "[ ]" }
        Write-Host "    $statusIcon " -ForegroundColor $statusColor -NoNewline
        Write-Host "$($node.Name)" -NoNewline
        Write-Host " - $($node.Desc)" -ForegroundColor Gray -NoNewline
        Write-Host " ($status)" -ForegroundColor $statusColor
    }
    
    $frontendStatus = Get-NodeStatus "frontend"
    $frontendColor = if ($frontendStatus -eq "Running") { "Green" } else { "DarkGray" }
    $frontendIcon = if ($frontendStatus -eq "Running") { "[*]" } else { "[ ]" }
    Write-Host "    $frontendIcon " -ForegroundColor $frontendColor -NoNewline
    Write-Host "Frontend" -NoNewline
    Write-Host " - Vue Dev Server" -ForegroundColor Gray -NoNewline
    Write-Host " ($frontendStatus)" -ForegroundColor $frontendColor
    
    Write-Host ""
    Write-ColorText "  Start Service:" "White"
    Write-Host "  -------------------------------"
    
    foreach ($node in $global:NodeConfig) {
        Write-MenuItem $node.Key "Start $($node.Name)"
    }
    Write-MenuItem "6" "Start Frontend"
    Write-Host ""
    Write-MenuItem "A" "Start all backend nodes"
    Write-MenuItem "F" "Start full stack (backend + frontend)"
    
    Write-Host ""
    Write-ColorText "  Management:" "White"
    Write-Host "  -------------------------------"
    Write-MenuItem "S" "Stop service"
    Write-MenuItem "X" "Stop all services"
    Write-MenuItem "L" "View logs"
    Write-MenuItem "R" "Refresh status"
    Write-MenuItem "Q" "Quit"
    
    Write-Host ""
}

# ==================== Main Loop ====================

function Main {
    if (!(Test-Path $global:AgentPath)) {
        Write-ColorText "Error: AgentServer dir not found: $global:AgentPath" "Red"
        Write-Host "Make sure script is in project root"
        return
    }
    
    while ($true) {
        Show-MainMenu
        $choice = Read-Host "Select"
        
        switch ($choice.ToUpper()) {
            "1" { Start-AgentNode "web"; Start-Sleep -Seconds 1 }
            "2" { Start-AgentNode "inference"; Start-Sleep -Seconds 1 }
            "3" { Start-AgentNode "data_sync"; Start-Sleep -Seconds 1 }
            "4" { Start-AgentNode "listener"; Start-Sleep -Seconds 1 }
            "5" { Start-AgentNode "backtest"; Start-Sleep -Seconds 1 }
            "6" { Start-Frontend; Start-Sleep -Seconds 1 }
            "A" { Start-AllNodes; Start-Sleep -Seconds 2 }
            "F" { 
                Start-AllNodes
                Start-Sleep -Seconds 1
                Start-Frontend
                Start-Sleep -Seconds 2
            }
            "S" { Show-StopMenu }
            "X" { Stop-AllServices; Start-Sleep -Seconds 1 }
            "L" { Show-LogMenu }
            "R" { continue }
            "Q" { 
                Write-Host ""
                Write-ColorText "  Bye!" "Cyan"
                Write-Host ""
                return 
            }
            default { 
                Write-ColorText "  Invalid choice, try again" "Red"
                Start-Sleep -Milliseconds 500
            }
        }
    }
}

# Run
Main
