// What a user writes in YAML
public class WorkflowDefinition
{
    public string? Name { get; set; }
    public List<StepDefinition>? Steps { get; set; }
}

public class StepDefinition
{
    public string? Name { get; set; }
    public List<string>? DependsOn { get; set; }
    public int? MaxAttempts { get; set; }
}

// What the status endpoint returns for each step
public record StepInfo(
    int Id,
    string Step,
    string Status,
    int Attempts,
    int MaxAttempts,
    string? ClaimedBy,
    string? LastError,
    string[] DependsOn);