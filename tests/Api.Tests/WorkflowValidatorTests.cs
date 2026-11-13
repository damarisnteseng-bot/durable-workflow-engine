using Xunit;

public class WorkflowValidatorTests
{
    // Small helpers so each test reads like a sentence
    private static StepDefinition Step(string name, params string[] dependsOn) =>
        new() { Name = name, DependsOn = dependsOn.ToList() };

    private static WorkflowDefinition Workflow(params StepDefinition[] steps) =>
        new() { Name = "test", Steps = steps.ToList() };

    [Fact]
    public void LinearWorkflow_IsValid()
    {
        var errors = WorkflowValidator.Validate(
            Workflow(Step("a"), Step("b", "a"), Step("c", "b")));

        Assert.Empty(errors);
    }

    [Fact]
    public void DiamondWorkflow_FanOutAndFanIn_IsNotMistakenForACycle()
    {
        var errors = WorkflowValidator.Validate(Workflow(
            Step("download"),
            Step("validate", "download"),
            Step("enrich", "validate"),
            Step("archive", "validate"),
            Step("notify", "enrich", "archive")));

        Assert.Empty(errors);
    }

    [Fact]
    public void ThreeStepCycle_IsDetected()
    {
        var errors = WorkflowValidator.Validate(
            Workflow(Step("a", "c"), Step("b", "a"), Step("c", "b")));

        Assert.Contains(errors, e => e.Contains("cycle"));
    }

    [Fact]
    public void TwoStepCycle_IsDetected()
    {
        var errors = WorkflowValidator.Validate(
            Workflow(Step("a", "b"), Step("b", "a")));

        Assert.Contains(errors, e => e.Contains("cycle"));
    }

    [Fact]
    public void CycleHiddenInsideAnOtherwiseHealthyWorkflow_IsDetected()
    {
        var errors = WorkflowValidator.Validate(Workflow(
            Step("start"),
            Step("middle", "start"),
            Step("x", "y"),
            Step("y", "x")));

        var message = Assert.Single(errors);
        Assert.Contains("x", message);
        Assert.Contains("y", message);
    }

    [Fact]
    public void StepDependingOnItself_IsRejected()
    {
        var errors = WorkflowValidator.Validate(Workflow(Step("a", "a")));

        Assert.Contains(errors, e => e.Contains("depends on itself"));
    }

    [Fact]
    public void UnknownDependency_IsRejected()
    {
        var errors = WorkflowValidator.Validate(Workflow(Step("a", "ghost")));

        Assert.Contains(errors, e => e.Contains("unknown step 'ghost'"));
    }

    [Fact]
    public void DuplicateStepNames_AreRejected()
    {
        var errors = WorkflowValidator.Validate(Workflow(Step("a"), Step("a")));

        Assert.Contains(errors, e => e.Contains("duplicate"));
    }

    [Fact]
    public void MissingWorkflowName_IsRejected()
    {
        var definition = new WorkflowDefinition
        {
            Name = null,
            Steps = new List<StepDefinition> { Step("a") }
        };

        var errors = WorkflowValidator.Validate(definition);

        Assert.Contains(errors, e => e.Contains("name"));
    }

    [Fact]
    public void WorkflowWithNoSteps_IsRejected()
    {
        var definition = new WorkflowDefinition
        {
            Name = "empty",
            Steps = new List<StepDefinition>()
        };

        var errors = WorkflowValidator.Validate(definition);

        Assert.Contains(errors, e => e.Contains("at least one step"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(11)]
    public void MaxAttemptsOutOfRange_IsRejected(int maxAttempts)
    {
        var step = Step("a");
        step.MaxAttempts = maxAttempts;

        var errors = WorkflowValidator.Validate(Workflow(step));

        Assert.Contains(errors, e => e.Contains("max_attempts"));
    }

    [Fact]
    public void SameDependencyListedTwice_IsStillValid()
    {
        var errors = WorkflowValidator.Validate(
            Workflow(Step("a"), Step("b", "a", "a")));

        Assert.Empty(errors);
    }

    [Fact]
    public void VeryLongChain_DoesNotOverflowOrSlowDown()
    {
        var steps = new List<StepDefinition> { Step("s0") };
        for (int i = 1; i < 2000; i++)
            steps.Add(Step($"s{i}", $"s{i - 1}"));

        var errors = WorkflowValidator.Validate(
            new WorkflowDefinition { Name = "long", Steps = steps });

        Assert.Empty(errors);
    }
}