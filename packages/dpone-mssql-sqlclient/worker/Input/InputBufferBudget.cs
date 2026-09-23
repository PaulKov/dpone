namespace Dpone.SqlClient.Input;

/// <summary>
/// Shared cap for owned input buffer payload/capacity, not CLR heap or RSS.
/// Account reference slots as 8 bytes, boxed Int64/Double/DateTime payload as 8,
/// byte arrays at full length, and strings as 2 bytes per UTF16 code unit.
/// Scratch and output overlap must both remain charged. Fixed admitted metadata,
/// ownership/reservation bookkeeping, object headers, hash/SDK internals and
/// unreachable GC-retained allocations are outside this metric. Arrow consumers
/// must charge actual backing capacities and growth/finalization overlap here too.
/// Ledger reserve/release is synchronized; each decoder, reader and row lease
/// retains single-consumer ownership and is not safe for concurrent operations.
/// </summary>
public sealed class InputBufferBudget
{
    private readonly object gate = new();
    private long usedBytes;
    /// <summary>Immutable positive accounted-byte ceiling.</summary>
    public long LimitBytes { get; }
    /// <summary>Current outstanding reservations, including live scratch.</summary>
    public long UsedBytes { get { lock (gate) return usedBytes; } }
    /// <summary>Admit an explicit finite cap; this is not an OS memory limit.</summary>
    public InputBufferBudget(long limitBytes)
    {
        if (limitBytes <= 0) throw new InvalidDataException("input.buffer_budget_invalid");
        LimitBytes = limitBytes;
    }
    /// <summary>Reserve before allocation; denial changes no accounting.</summary>
    public InputBufferReservation Reserve(long bytes)
    {
        if (bytes < 0) throw new InvalidDataException("input.buffer_budget_invalid");
        lock (gate)
        {
            if (bytes > LimitBytes - usedBytes) throw new InvalidDataException("input.buffer_budget_exceeded");
            var reservation = new InputBufferReservation(this, bytes);
            usedBytes = checked(usedBytes + bytes);
            return reservation;
        }
    }
    internal void Release(long bytes) { lock (gate) usedBytes = checked(usedBytes - bytes); }
}

/// <summary>Exactly-once release of a reservation; callers cannot invent releases.</summary>
public sealed class InputBufferReservation : IDisposable
{
    private InputBufferBudget? owner;
    internal InputBufferReservation? Next;
    /// <summary>Original immutable accounted byte amount.</summary>
    public long Bytes { get; }
    internal InputBufferReservation(InputBufferBudget owner, long bytes) => (this.owner, Bytes) = (owner, bytes);
    /// <summary>Release once; repeated disposal has no effect.</summary>
    public void Dispose() => Interlocked.Exchange(ref owner, null)?.Release(Bytes);
}
