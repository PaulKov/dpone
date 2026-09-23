namespace Dpone.SqlClient.Bulk;

/// <summary>Finite row and retained decoded-buffer budgets for one input representation.</summary>
public sealed class BulkInputPolicy
{
    /// <summary>Maximum rows per SDK transaction and decoded Arrow batch.</summary>
    public int BatchRows { get; }
    /// <summary>Maximum accounted retained decoded input bytes; not total process RSS.</summary>
    public long MaxInputBatchBytes { get; }
    /// <summary>Validate explicit finite component budgets.</summary>
    public BulkInputPolicy(int batchRows, long maxInputBatchBytes)
    {
        if (batchRows <= 0 || maxInputBatchBytes is < 1048576 or > 268435456)
            throw new InvalidDataException("bulk.input_limits");
        (BatchRows, MaxInputBatchBytes) = (batchRows, maxInputBatchBytes);
    }
}

/// <summary>Existing stage coordinates; upstream owns admission, identity and one-shot grant.</summary>
public sealed class BulkDestination
{
    /// <summary>Quoted three-part name; never an authored SQL fragment.</summary>
    public string QualifiedName { get; }
    /// <summary>Admit exact identifiers without constructing a connection or issuing DDL.</summary>
    public BulkDestination(string database, string schema, string table)
        => QualifiedName = string.Join('.', new[] { database, schema, table }.Select(Quote));
    private static string Quote(string value)
    {
        if (string.IsNullOrEmpty(value) || value.Length > 128 || value.IndexOf('\0') >= 0)
            throw new InvalidDataException("bulk.destination_identifier");
        return "[" + value.Replace("]", "]]") + "]";
    }
}

/// <summary>Original nonrenewable monotonic operation deadline supplied by the supervisor.</summary>
public sealed class BulkDeadline
{
    private readonly TimeProvider clock;
    private readonly long expiresAt;
    /// <summary>Bind the supervisor's absolute timestamp and the same clock domain.</summary>
    public BulkDeadline(TimeProvider clock, long expiresAt)
        => (this.clock, this.expiresAt) = (clock ?? throw new ArgumentNullException(nameof(clock)), expiresAt);
    /// <summary>Return remaining budget; expired deadlines cannot start another operation.</summary>
    public TimeSpan Remaining()
    {
        var remaining = clock.GetElapsedTime(clock.GetTimestamp(), expiresAt);
        if (remaining <= TimeSpan.Zero) throw new TimeoutException("bulk.deadline_expired");
        return remaining;
    }
    /// <summary>SDK whole-second timeout never exceeds the original remaining budget.</summary>
    public int SdkTimeoutSeconds()
    {
        double seconds = Math.Floor(Remaining().TotalSeconds);
        if (seconds < 1) throw new TimeoutException("bulk.deadline_expired");
        return (int)Math.Min(int.MaxValue, seconds);
    }
    internal CancellationTokenSource CreateCancellation() => new(Remaining(), clock);
}
