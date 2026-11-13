using Npgsql;

var workerName = args.Length > 0 && !args[0].StartsWith("--")
    ? args[0]
    : $"worker-{Environment.ProcessId}";
bool heartbeatsOn = !args.Contains("--no-heartbeat");

var connectionString =
    "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";

await using var dataSource = NpgsqlDataSource.Create(connectionString);

Console.WriteLine($"[{workerName}] started, heartbeats {(heartbeatsOn ? "ON" : "OFF")} (Ctrl+C to stop)");

// Renews the lease every 3 seconds while a job is running.
// If the update touches 0 rows, we no longer own the job, so we cancel the work.
async Task HeartbeatLoop(int jobId, CancellationTokenSource cts)
{
    try
    {
        while (!cts.IsCancellationRequested)
        {
            await Task.Delay(3000, cts.Token);

            await using var cmd = dataSource.CreateCommand(@"
                UPDATE jobs
                SET lease_expires_at = now() + interval '8 seconds'
                WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'");
            cmd.Parameters.AddWithValue(jobId);
            cmd.Parameters.AddWithValue(workerName);
            int rows = await cmd.ExecuteNonQueryAsync();

            if (rows == 0)
            {
                Console.WriteLine($"[{workerName}] heartbeat: lost job {jobId}, abandoning it");
                cts.Cancel();
                return;
            }

            Console.WriteLine($"[{workerName}] heartbeat: renewed lease on job {jobId}");
        }
    }
    catch (OperationCanceledException)
    {
        // Normal: the job finished and we asked the heartbeat to stop
    }
}

bool idle = false;

while (true)
{
    await using var claimCmd = dataSource.CreateCommand(@"
        UPDATE jobs
        SET status = 'RUNNING',
            claimed_by = $1,
            lease_expires_at = now() + interval '8 seconds',
            attempts = attempts + 1
        WHERE id = (
            SELECT id FROM jobs
            WHERE (status = 'PENDING' AND run_after <= now())
               OR (status = 'RUNNING'
                   AND (lease_expires_at IS NULL OR lease_expires_at < now()))
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, attempts, max_attempts");
    claimCmd.Parameters.AddWithValue(workerName);

    int jobId = 0, attempt = 0, maxAttempts = 0;
    bool found = false;

    await using (var reader = await claimCmd.ExecuteReaderAsync())
    {
        if (await reader.ReadAsync())
        {
            found = true;
            jobId = reader.GetInt32(0);
            attempt = reader.GetInt32(1);
            maxAttempts = reader.GetInt32(2);
        }
    }

    if (!found)
    {
        if (!idle)
        {
            Console.WriteLine($"[{workerName}] nothing to do, waiting...");
            idle = true;
        }
        await Task.Delay(1000);
        continue;
    }

    idle = false;

    // Job 2 is a deliberately SLOW job (20 seconds), longer than the 8-second lease
    int workMs = jobId == 2 ? 20000 : 1000;
    Console.WriteLine($"[{workerName}] claimed job {jobId} (attempt {attempt}/{maxAttempts}), work takes {workMs / 1000}s");

    using var jobCts = new CancellationTokenSource();
    Task heartbeat = heartbeatsOn ? HeartbeatLoop(jobId, jobCts) : Task.CompletedTask;

    bool abandoned = false;
    try
    {
        await Task.Delay(workMs, jobCts.Token);   // the "work"
    }
    catch (OperationCanceledException)
    {
        abandoned = true;
    }

    jobCts.Cancel();      // stop the heartbeat
    await heartbeat;

    if (abandoned)
    {
        Console.WriteLine($"[{workerName}] stopped working on job {jobId} (lease lost)");
        continue;
    }

    // Simulated failures from Day 6: multiples of 5 always fail,
    // other multiples of 3 fail on their first attempt only
    bool shouldFail = jobId % 5 == 0 || (jobId % 3 == 0 && attempt == 1);

    if (!shouldFail)
    {
        await using var doneCmd = dataSource.CreateCommand(
            "UPDATE jobs SET status = 'DONE' WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'");
        doneCmd.Parameters.AddWithValue(jobId);
        doneCmd.Parameters.AddWithValue(workerName);
        int rows = await doneCmd.ExecuteNonQueryAsync();

        Console.WriteLine(rows == 1
            ? $"[{workerName}] finished job {jobId}"
            : $"[{workerName}] LOST job {jobId}, discarding result");
        continue;
    }

    const string error = "simulated failure";

    if (attempt >= maxAttempts)
    {
        await using var deadCmd = dataSource.CreateCommand(
            "UPDATE jobs SET status = 'DEAD', last_error = $3 WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'");
        deadCmd.Parameters.AddWithValue(jobId);
        deadCmd.Parameters.AddWithValue(workerName);
        deadCmd.Parameters.AddWithValue(error);
        await deadCmd.ExecuteNonQueryAsync();

        Console.WriteLine($"[{workerName}] job {jobId} FAILED for good, marked DEAD");
    }
    else
    {
        double seconds = Math.Min(30, 2 * Math.Pow(2, attempt - 1))
                         * (0.5 + Random.Shared.NextDouble());

        await using var retryCmd = dataSource.CreateCommand(@"
            UPDATE jobs
            SET status = 'PENDING',
                claimed_by = NULL,
                lease_expires_at = NULL,
                run_after = now() + make_interval(secs => $3::float8),
                last_error = $4
            WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'");
        retryCmd.Parameters.AddWithValue(jobId);
        retryCmd.Parameters.AddWithValue(workerName);
        retryCmd.Parameters.AddWithValue(seconds);
        retryCmd.Parameters.AddWithValue(error);
        await retryCmd.ExecuteNonQueryAsync();

        Console.WriteLine($"[{workerName}] job {jobId} failed, will retry in {seconds:F1}s");
    }
}