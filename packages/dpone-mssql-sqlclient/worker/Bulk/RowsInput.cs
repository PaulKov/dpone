using Dpone.SqlClient.Input;

namespace Dpone.SqlClient.Bulk;

/// <summary>Direct rows representation with pre-allocation decoded-buffer admission.</summary>
public sealed class RowsInput : IDisposable
{
    /// <summary>Shared ledger for input buffers, retained row and decoder scratch.</summary>
    public InputBufferBudget Budget { get; }
    /// <summary>Exact typed streaming reader; SDK batching is independently finite.</summary>
    public NativeInputReader Reader { get; }
    /// <summary>Bind the stream without opening paths, reading values or taking stream ownership.</summary>
    public RowsInput(Stream stream, NativeInputContract contract, BulkInputPolicy policy, CancellationToken cancellation = default)
        : this(stream, contract, policy, new InputBufferBudget(policy.MaxInputBatchBytes), cancellation) { }
    /// <summary>
    /// Share the original admitted ledger with the buffered input transport.
    /// The explicit cancellation argument preserves the original constructor's
    /// unambiguous four-argument call shape. Disposal releases only reader leases.
    /// </summary>
    public RowsInput(Stream stream, NativeInputContract contract, BulkInputPolicy policy,
        InputBufferBudget budget, CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(budget);
        if (budget.LimitBytes != policy.MaxInputBatchBytes) throw new InvalidDataException("bulk.budget_identity");
        Budget = budget;
        Reader = new NativeInputReader(stream, contract, cancellation, Budget);
    }
    /// <summary>Release input allocations while leaving the injected stream open.</summary>
    public void Dispose() => Reader.Dispose();
}
