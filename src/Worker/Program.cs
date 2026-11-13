using Npgsql;

var workerName = args.Length > 0 ? args[0] : $"worker-{Environment.ProcessId}";

var connectionString =
    "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";

await using var dataSource = NpgsqlDataSource.Create(connectionString);

Console.WriteLine($"[{workerName}] started (press Ctrl+C to stop)");

bool idle = false;

while (true)
{
    // Claim a job that is ready to run (PENDING and its retry time has arrived),
    // or one whose lease expired (its worker died). Every claim counts as an attempt.
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
    Console.WriteLine($"[{workerName}] claimed job {jobId} (attempt {attempt}/{maxAttempts})");

    // Pretend to do 1 second of work
    await Task.Delay(1000);

    // Simulated failures, so we can watch retries happen:
    //  - jobs whose id is a multiple of 5 ALWAYS fail (a "poison" job)
    //  - other multiples of 3 fail on their first attempt only
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
        // Out of attempts: park it as DEAD
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
        // Exponential backoff with jitter: about 2s, 4s, 8s... capped at 30s,
        // multiplied by a random factor between 0.5 and 1.5
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