using Npgsql;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

var builder = WebApplication.CreateBuilder(args);

var connectionString =
    Environment.GetEnvironmentVariable("DB_CONNECTION")
    ?? "Host=localhost;Port=5433;Username=workflow;Password=workflow_dev_pw;Database=workflow";
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));

var app = builder.Build();

var yamlReader = new DeserializerBuilder()
    .WithNamingConvention(UnderscoredNamingConvention.Instance)   // depends_on -> DependsOn
    .IgnoreUnmatchedProperties()
    .Build();

// POST /workflows : submit a workflow written in YAML
app.MapPost("/workflows", async (HttpRequest request, NpgsqlDataSource db) =>
{
    using var bodyReader = new StreamReader(request.Body);
    string body = await bodyReader.ReadToEndAsync();

    WorkflowDefinition? definition;
    try
    {
        definition = yamlReader.Deserialize<WorkflowDefinition>(body);
    }
    catch (YamlException ex)
    {
        return Results.BadRequest(new { errors = new[] { $"invalid YAML: {ex.Message}" } });
    }

    if (definition is null)
        return Results.BadRequest(new { errors = new[] { "request body is empty" } });

    List<string> errors = WorkflowValidator.Validate(definition);
    if (errors.Count > 0)
        return Results.BadRequest(new { errors });

    // Store the whole workflow in ONE transaction: all steps and dependencies
    // become visible to workers together, or not at all.
    await using var conn = await db.OpenConnectionAsync();
    await using var tx = await conn.BeginTransactionAsync();

    int workflowId;
    await using (var idCmd = new NpgsqlCommand("SELECT nextval('workflow_id_seq')", conn, tx))
        workflowId = Convert.ToInt32(await idCmd.ExecuteScalarAsync());

    var jobIds = new Dictionary<string, int>();
    foreach (var step in definition.Steps!)
    {
        await using var cmd = new NpgsqlCommand(@"
            INSERT INTO jobs (workflow_id, workflow_name, step_name, max_attempts)
            VALUES ($1, $2, $3, $4) RETURNING id", conn, tx);
        cmd.Parameters.AddWithValue(workflowId);
        cmd.Parameters.AddWithValue(definition.Name!);
        cmd.Parameters.AddWithValue(step.Name!);
        cmd.Parameters.AddWithValue(step.MaxAttempts ?? 3);
        jobIds[step.Name!] = Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    foreach (var step in definition.Steps!)
    {
        foreach (var dep in (step.DependsOn ?? new List<string>()).Distinct())
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO job_dependencies (job_id, depends_on_id) VALUES ($1, $2)", conn, tx);
            cmd.Parameters.AddWithValue(jobIds[step.Name!]);
            cmd.Parameters.AddWithValue(jobIds[dep]);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    await tx.CommitAsync();

    return Results.Created($"/workflows/{workflowId}",
        new { workflowId, name = definition.Name, steps = definition.Steps.Count });
});

// GET /workflows/{id} : see the status of every step
app.MapGet("/workflows/{id:int}", async (int id, NpgsqlDataSource db) =>
{
    var steps = new List<StepInfo>();
    string? name = null;

    await using var cmd = db.CreateCommand(@"
        SELECT j.id, j.step_name, j.status, j.attempts, j.max_attempts,
               j.claimed_by, j.last_error, j.workflow_name,
               COALESCE(
                 (SELECT array_agg(p.step_name ORDER BY p.id)
                  FROM job_dependencies d JOIN jobs p ON p.id = d.depends_on_id
                  WHERE d.job_id = j.id),
                 ARRAY[]::text[]) AS depends_on
        FROM jobs j
        WHERE j.workflow_id = $1
        ORDER BY j.id");
    cmd.Parameters.AddWithValue(id);

    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        name ??= reader.IsDBNull(7) ? null : reader.GetString(7);
        steps.Add(new StepInfo(
            reader.GetInt32(0),
            reader.IsDBNull(1) ? "?" : reader.GetString(1),
            reader.GetString(2),
            reader.GetInt32(3),
            reader.GetInt32(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.GetFieldValue<string[]>(8)));
    }

    if (steps.Count == 0)
        return Results.NotFound(new { error = $"workflow {id} not found" });

    string status =
        steps.Any(s => s.Status is "PENDING" or "RUNNING") ? "IN_PROGRESS"
        : steps.Any(s => s.Status is "DEAD" or "CANCELLED") ? "FAILED"
        : "SUCCEEDED";

    return Results.Ok(new { workflowId = id, name, status, steps });
});

app.Run("http://localhost:5080");