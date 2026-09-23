namespace Dpone.SqlClient.Input;

/// <summary>
/// Exclusive lifetime of one decoded row and its retained payload reservations.
/// Values are borrowed until disposal and must not escape into unaccounted storage.
/// Transfer the lease itself when retaining a row. Like its reader, this owner is
/// single-consumer; concurrent getters, retention and disposal are unsupported.
/// </summary>
public sealed class DecodedRowLease : IDisposable
{
    private object[]? values;
    private InputBufferReservation? reservations;
    /// <summary>Borrowed row slots; disposal clears references and invalidates access.</summary>
    public object[] Values => values ?? throw new ObjectDisposedException(nameof(DecodedRowLease));
    /// <summary>Currently owned reference/scalar/string bytes; excludes decoder scratch.</summary>
    public long RetainedBytes { get; private set; }
    private DecodedRowLease(object[] values, InputBufferReservation? slots)
    {
        this.values = values;
        Retain(slots);
    }
    internal static DecodedRowLease Create(int count, InputBufferBudget? budget)
    {
        InputBufferReservation? slots = budget?.Reserve(checked(8L * count));
        try { return new DecodedRowLease(new object[count], slots); }
        catch { slots?.Dispose(); throw; }
    }
    internal void Retain(InputBufferReservation? reservation)
    {
        if (reservation is null) return;
        RetainedBytes = checked(RetainedBytes + reservation.Bytes);
        reservation.Next = reservations;
        reservations = reservation;
    }
    internal object[] DetachUnbudgetedValues()
    {
        if (reservations is not null) throw new InvalidOperationException("input.lease_required");
        object[] detached = Values;
        values = null;
        return detached;
    }
    /// <summary>Drop all row references before releasing their accounting, exactly once.</summary>
    public void Dispose()
    {
        if (values is null) return;
        Array.Clear(values);
        values = null;
        while (reservations is not null)
        {
            InputBufferReservation next = reservations;
            reservations = next.Next;
            next.Next = null;
            next.Dispose();
        }
        RetainedBytes = 0;
    }
}
