using Npgsql;

var workerName = args.Length > 0 && !args[0].StartsWith("--")
    ? args[0]
    : $"worker-{Environment.ProcessId}";

var connectionString =
    Environment.GetEnvironmentVariable("DB_CONNECTION")
    ?? "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";

await using var dataSource = NpgsqlDataSource.Create(connectionString);

Console.WriteLine($"[{workerName}] started (Ctrl+C to stop)");

// Renews the lease every 3 seconds while a step is running.
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
        }
    }
    catch (OperationCanceledException)
    {
        // Normal: the step finished and we asked the heartbeat to stop
    }
}

bool idle = false;

while (true)
{
    // Housekeeping 1: a job whose lease expired with no attempts left is DEAD
    await using var reapCmd = dataSource.CreateCommand(@"
        UPDATE jobs
        SET status = 'DEAD', last_error = 'lease expired with no attempts left'
        WHERE status = 'RUNNING'
          AND lease_expires_at < now()
          AND attempts >= max_attempts");
    int reaped = await reapCmd.ExecuteNonQueryAsync();
    if (reaped > 0)
        Console.WriteLine($"[{workerName}] marked {reaped} abandoned job(s) as DEAD");

    // Housekeeping 2: if any parent step is DEAD or CANCELLED, this step can
    // never run, so cancel it. This cascades one level per loop.
    await using var cancelCmd = dataSource.CreateCommand(@"
        UPDATE jobs
        SET status = 'CANCELLED', last_error = 'a step it depends on failed'
        WHERE status = 'PENDING'
          AND EXISTS (
              SELECT 1 FROM job_dependencies d
              JOIN jobs p ON p.id = d.depends_on_id
              WHERE d.job_id = jobs.id AND p.status IN ('DEAD', 'CANCELLED'))");
    int cancelled = await cancelCmd.ExecuteNonQueryAsync();
    if (cancelled > 0)
        Console.WriteLine($"[{workerName}] cancelled {cancelled} step(s) because a dependency failed");

    // Claim a step that is ready: all parents DONE, retry time reached.
    // Or rescue a step whose lease expired.
    await using var claimCmd = dataSource.CreateCommand(@"
        UPDATE jobs
        SET status = 'RUNNING',
            claimed_by = $1,
            lease_expires_at = now() + interval '8 seconds',
            attempts = attempts + 1
        WHERE id = (
            SELECT j.id FROM jobs j
            WHERE (j.status = 'PENDING'
                   AND j.run_after <= now()
                   AND NOT EXISTS (
                       SELECT 1 FROM job_dependencies d
                       JOIN jobs p ON p.id = d.depends_on_id
                       WHERE d.job_id = j.id AND p.status <> 'DONE'))
               OR (j.status = 'RUNNING'
                   AND (j.lease_expires_at IS NULL OR j.lease_expires_at < now()))
            ORDER BY j.id
            FOR UPDATE OF j SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, attempts, max_attempts, step_name, workflow_id");
    claimCmd.Parameters.AddWithValue(workerName);

    int jobId = 0, attempt = 0, maxAttempts = 0, workflowId = 0;
    string stepName = "?";
    bool found = false;

    await using (var reader = await claimCmd.ExecuteReaderAsync())
    {
        if (await reader.ReadAsync())
        {
            found = true;
            jobId = reader.GetInt32(0);
            attempt = reader.GetInt32(1);
            maxAttempts = reader.GetInt32(2);
            stepName = reader.IsDBNull(3) ? "?" : reader.GetString(3);
            workflowId = reader.IsDBNull(4) ? 0 : reader.GetInt32(4);
        }
    }

    if (!found)
    {
        if (!idle)
        {
            Console.WriteLine($"[{workerName}] nothing ready, waiting...");
            idle = true;
        }
        await Task.Delay(1000);
        continue;
    }

    idle = false;
    Console.WriteLine($"[{workerName}] started wf{workflowId}/{stepName} (job {jobId}, attempt {attempt}/{maxAttempts})");
    await using var execCmd = dataSource.CreateCommand(
        "INSERT INTO step_executions (job_id, worker, attempt) VALUES ($1, $2, $3)");
    execCmd.Parameters.AddWithValue(jobId);
    execCmd.Parameters.AddWithValue(workerName);
    execCmd.Parameters.AddWithValue(attempt);
    await execCmd.ExecuteNonQueryAsync();

    using var jobCts = new CancellationTokenSource();
    Task heartbeat = HeartbeatLoop(jobId, jobCts);

    bool abandoned = false;
    try
    {
              await Task.Delay(750, jobCts.Token);   // first half of the work

        // The step's side effect (think "send the email"), made idempotent with a
        // stable key. If this step runs twice, the second request is recognised
        // and the side effect is NOT repeated.
        string idempotencyKey = $"wf{workflowId}:{stepName}";
        await using var effectCmd = dataSource.CreateCommand(@"
            INSERT INTO effects (idempotency_key, job_id, step_name, worker)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (idempotency_key)
            DO UPDATE SET times_requested = effects.times_requested + 1
            RETURNING times_requested");
        effectCmd.Parameters.AddWithValue(idempotencyKey);
        effectCmd.Parameters.AddWithValue(jobId);
        effectCmd.Parameters.AddWithValue(stepName);
        effectCmd.Parameters.AddWithValue(workerName);
        int timesRequested = (int)(await effectCmd.ExecuteScalarAsync())!;

        if (timesRequested > 1)
            Console.WriteLine($"[{workerName}] {idempotencyKey} already applied (request #{timesRequested}), skipping side effect");

        await Task.Delay(750, jobCts.Token);   // second half of the work // the "work"
    }
    catch (OperationCanceledException)
    {
        abandoned = true;
    }

    jobCts.Cancel();
    await heartbeat;

    if (abandoned)
    {
        Console.WriteLine($"[{workerName}] stopped working on job {jobId} (lease lost)");
        continue;
    }

    // Simulated failures, driven by the step's name:
    //  - names ending in -poison always fail
    //  - names ending in -flaky fail on the first attempt only
    bool shouldFail = stepName.EndsWith("-poison")
                      || (stepName.EndsWith("-flaky") && attempt == 1);

    if (!shouldFail)
    {
        await using var doneCmd = dataSource.CreateCommand(
                        "UPDATE jobs SET status = 'DONE', finished_at = now() WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'");
        doneCmd.Parameters.AddWithValue(jobId);
        doneCmd.Parameters.AddWithValue(workerName);
        int rows = await doneCmd.ExecuteNonQueryAsync();

        Console.WriteLine(rows == 1
            ? $"[{workerName}] finished wf{workflowId}/{stepName}"
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

        Console.WriteLine($"[{workerName}] wf{workflowId}/{stepName} FAILED for good, marked DEAD");
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

        Console.WriteLine($"[{workerName}] wf{workflowId}/{stepName} failed, will retry in {seconds:F1}s");
    }
}