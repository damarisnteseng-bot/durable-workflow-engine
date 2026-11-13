$workerCount = 4
$killEverySeconds = 5
$timeoutSeconds = 300
$dll = "src\Worker\bin\Debug\net10.0\Worker.dll"

New-Item -ItemType Directory -Force -Path logs | Out-Null

function Query($sql) {
    (docker exec workflow-postgres psql -U workflow -d workflow -t -A -c $sql).Trim()
}

$script:generation = 0
function Start-Worker($slot) {
    $script:generation++
    $name = "w$slot-g$($script:generation)"
    Start-Process -FilePath dotnet -ArgumentList $dll, $name -PassThru `
        -WindowStyle Hidden -RedirectStandardOutput "logs\$name.log"
}

$workers = @{}
for ($i = 1; $i -le $workerCount; $i++) { $workers[$i] = Start-Worker $i }
Write-Host "Started $workerCount workers. Killing one every $killEverySeconds seconds."

$kills = 0
$start = Get-Date

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

foreach ($w in $workers.Values) { Stop-Process -Id $w.Id -Force -ErrorAction SilentlyContinue }

$seconds = [int]((Get-Date) - $start).TotalSeconds
Write-Host "Done in $seconds seconds with $kills worker kills."