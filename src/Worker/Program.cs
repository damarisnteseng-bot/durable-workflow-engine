using Npgsql;

var connectionString =
    "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";

await using var dataSource = NpgsqlDataSource.Create(connectionString);

// 1. Find one PENDING job
await using var selectCmd = dataSource.CreateCommand(
    "SELECT id FROM jobs WHERE status = 'PENDING' ORDER BY id LIMIT 1");
var result = await selectCmd.ExecuteScalarAsync();

if (result is null)
{
    Console.WriteLine("No pending jobs.");
    return;
}

int jobId = (int)result;
Console.WriteLine($"Picked up job {jobId}");

// 2. Mark it DONE
await using var updateCmd = dataSource.CreateCommand(
    "UPDATE jobs SET status = 'DONE' WHERE id = $1");
updateCmd.Parameters.AddWithValue(jobId);
await updateCmd.ExecuteNonQueryAsync();

Console.WriteLine($"Job {jobId} marked DONE");