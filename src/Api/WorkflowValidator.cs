public static class WorkflowValidator
{
    // Returns a list of problems. An empty list means the workflow is valid.
    public static List<string> Validate(WorkflowDefinition definition)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(definition.Name))
            errors.Add("workflow 'name' is required");

        var steps = definition.Steps;
        if (steps is null || steps.Count == 0)
        {
            errors.Add("at least one step is required");
            return errors;
        }

        // 1. Every step needs a unique name, and sensible settings
        var names = new HashSet<string>();
        foreach (var step in steps)
        {
            if (string.IsNullOrWhiteSpace(step.Name))
            {
                errors.Add("every step needs a 'name'");
                continue;
            }
            if (!names.Add(step.Name))
                errors.Add($"duplicate step name '{step.Name}'");
            if (step.MaxAttempts is < 1 or > 10)
                errors.Add($"step '{step.Name}': max_attempts must be between 1 and 10");
        }

        // 2. Every dependency must point at a real step (and not at itself)
        foreach (var step in steps)
        {
            foreach (var dep in step.DependsOn ?? new List<string>())
            {
                if (dep == step.Name)
                    errors.Add($"step '{step.Name}' depends on itself");
                else if (!names.Contains(dep))
                    errors.Add($"step '{step.Name}' depends on unknown step '{dep}'");
            }
        }

        // Only check for cycles once the graph is structurally valid
        if (errors.Count > 0) return errors;

        // 3. Cycle detection (Kahn's algorithm)
        var waitingOn = steps.ToDictionary(
            s => s.Name!, s => (s.DependsOn ?? new List<string>()).Distinct().Count());

        var children = names.ToDictionary(n => n, _ => new List<string>());
        foreach (var step in steps)
            foreach (var dep in (step.DependsOn ?? new List<string>()).Distinct())
                children[dep].Add(step.Name!);

        var ready = new Queue<string>(waitingOn.Where(kv => kv.Value == 0).Select(kv => kv.Key));
        int processed = 0;

        while (ready.Count > 0)
        {
            string current = ready.Dequeue();
            processed++;
            foreach (var child in children[current])
                if (--waitingOn[child] == 0)
                    ready.Enqueue(child);
        }

        if (processed < names.Count)
        {
            var stuck = waitingOn.Where(kv => kv.Value > 0).Select(kv => kv.Key);
            errors.Add("dependency cycle detected; steps involved in or blocked by it: "
                       + string.Join(", ", stuck));
        }

        return errors;
    }
}