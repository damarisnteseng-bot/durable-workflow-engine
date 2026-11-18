# Chaos test: run workers, kill a random one every few seconds,
# and wait for the queue to drain.
$workerCount = 4
$killEverySeconds = 5
$timeoutSeconds = 300

# Always work from the folder this script lives in
Set-Location $PSScriptRoot
$dll = "src\Worker\bin\Debug\net10.0\Worker.dll"

if (-not (Test-Path $dll)) {
    Write-Host "Worker.dll not found. Run 'dotnet build' in this folder first."
    exit 1
}

New-Item -ItemType Directory -Force -Path logs | Out-Null
Remove-Item "logs\*.log" -ErrorAction SilentlyContinue

# Runs a SQL query and stops with a clear error if the database can't answer
function Query($sql) {
    $output = docker exec workflow-postgres psql -U workflow -d workflow -t -A -c $sql
    if ($LASTEXITCODE -ne 0 -or $null -eq $output) {
        throw "Database query failed. Is the postgres container running and the schema loaded?"
    }
    return ("$output").Trim()
}

$pending = [int](Query "SELECT count(*) FROM jobs WHERE status = 'PENDING'")
if ($pending -eq 0) {
    Write-Host "There are no PENDING steps. Load the demo data first (see the README, 'Chaos test')."
    exit 1
}
Write-Host "Starting with $pending pending steps."

$script:generation = 0
function Start-Worker($slot) {
    $script:generation++
    $name = "w$slot-g$($script:generation)"
    Start-Process -FilePath dotnet -ArgumentList $dll, $name -PassThru `
        -WindowStyle Hidden -RedirectStandardOutput "logs\$name.log"
}

$workers = @{}
$kills = 0
$start = Get-Date

try {
    for ($i = 1; $i -le $workerCount; $i++) { $workers[$i] = Start-Worker $i }
    Write-Host "Started $workerCount workers. Killing one every $killEverySeconds seconds."

    while ($true) {
        Start-Sleep -Seconds $killEverySeconds

        $remaining = [int](Query "SELECT count(*) FROM jobs WHERE status IN ('PENDING','RUNNING')")
        if ($remaining -eq 0) { Write-Host "All steps finished."; break }

        if (((Get-Date) - $start).TotalSeconds -gt $timeoutSeconds) {
            Write-Host "TIMEOUT: $remaining steps still unfinished"
            break
        }

        $slot = Get-Random -InputObject @($workers.Keys)
        Stop-Process -Id $workers[$slot].Id -Force
        $kills++
        Write-Host "Killed a worker (kill #$kills), $remaining steps remaining"
        $workers[$slot] = Start-Worker $slot
    }
}
finally {
    # Never leave workers running, even if something above failed
    foreach ($w in $workers.Values) {
        Stop-Process -Id $w.Id -Force -ErrorAction SilentlyContinue
    }
}

$seconds = [int]((Get-Date) - $start).TotalSeconds
Write-Host "Done in $seconds seconds with $kills worker kills."