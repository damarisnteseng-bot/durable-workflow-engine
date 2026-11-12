using Npgsql;

var workerName = args.Length > 0 ? args[0] : $"worker-{Environment.ProcessId}";

var connectionString =
    "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";

await using var dataSource = NpgsqlDataSource.Create(connectionString);

Console.WriteLine($"[{workerName}] started");

while (true)
{
    // Claim one job. SKIP LOCKED means: if another worker is already
    // grabbing a row, skip it and take the next one instead of waiting.
    await using var claimCmd = dataSource.CreateCommand(@"
        UPDATE jobs
        SET status = 'RUNNING', claimed_by = $1
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = 'PENDING'
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id");
    claimCmd.Parameters.AddWithValue(workerName);

    var result = await claimCmd.ExecuteScalarAsync();

    if (result is null)
    {
        Console.WriteLine($"[{workerName}] no more pending jobs, stopping");
        break;
    }

    int jobId = (int)result;
    Console.WriteLine($"[{workerName}] claimed job {jobId}");

    // Pretend to do some work
    await Task.Delay(500);

    await using var doneCmd = dataSource.CreateCommand(
        "UPDATE jobs SET status = 'DONE' WHERE id = $1");
    doneCmd.Parameters.AddWithValue(jobId);
    await doneCmd.ExecuteNonQueryAsync();

    Console.WriteLine($"[{workerName}] finished job {jobId}");
}