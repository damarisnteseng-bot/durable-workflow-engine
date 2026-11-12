using Npgsql;

var workerName = args.Length > 0 ? args[0] : $"worker-{Environment.ProcessId}";

var connectionString =
    "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";

await using var dataSource = NpgsqlDataSource.Create(connectionString);

Console.WriteLine($"[{workerName}] started (press Ctrl+C to stop)");

bool idle = false;

while (true)
{
    // Claim a job that is either PENDING, or RUNNING with an expired lease
    // (meaning the worker that had it has died). Set a fresh 8-second lease.
    await using var claimCmd = dataSource.CreateCommand(@"
        UPDATE jobs
        SET status = 'RUNNING',
            claimed_by = $1,
            lease_expires_at = now() + interval '8 seconds'
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = 'PENDING'
               OR (status = 'RUNNING'
                   AND (lease_expires_at IS NULL OR lease_expires_at < now()))
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id");
    claimCmd.Parameters.AddWithValue(workerName);

    var result = await claimCmd.ExecuteScalarAsync();

    if (result is null)
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
    int jobId = (int)result;
    Console.WriteLine($"[{workerName}] claimed job {jobId}");

    // Pretend to do 2 seconds of work
    await Task.Delay(2000);

    // Only mark DONE if we still own the job
    await using var doneCmd = dataSource.CreateCommand(
        "UPDATE jobs SET status = 'DONE' WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'");
    doneCmd.Parameters.AddWithValue(jobId);
    doneCmd.Parameters.AddWithValue(workerName);
    int rows = await doneCmd.ExecuteNonQueryAsync();

    if (rows == 1)
        Console.WriteLine($"[{workerName}] finished job {jobId}");
    else
        Console.WriteLine($"[{workerName}] LOST job {jobId} (someone else took it), discarding result");
}