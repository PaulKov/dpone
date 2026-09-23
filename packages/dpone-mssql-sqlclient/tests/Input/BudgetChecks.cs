using System.Security.Cryptography;
using System.Text;
using Dpone.SqlClient.Input;

internal static class BudgetChecks
{
    internal static void Run()
    {
        Ledger();
        TextPeak();
        Lifetimes();
        FailureControls();
        NullableAndWide();
        SharedOwnership();
        Console.WriteLine("PASS decoded budget: exact peaks, preallocation, lease transfer, cancellation, EOF, failure, compatibility");
    }
    private static void Ledger()
    {
        var budget = new InputBufferBudget(long.MaxValue);
        var reservation = budget.Reserve(long.MaxValue);
        Reject(() => budget.Reserve(1));
        Check(budget.UsedBytes == long.MaxValue, "failed_reserve_unchanged");
        reservation.Dispose(); reservation.Dispose();
        Check(budget.UsedBytes == 0, "release_once");
        Reject(() => budget.Reserve(-1));
        Reject(() => new InputBufferBudget(0));
    }
    private static void TextPeak()
    {
        byte[] bytes = Text("abcdefghij");
        var budget = new InputBufferBudget(49);
        using (var reader = Reader(bytes, budget))
        {
            Check(reader.HasRows && budget.UsedBytes == 29, "retained_text");
            Check(reader.HasRows && budget.UsedBytes == 29, "lookahead_once");
            Check(reader.Read() && budget.UsedBytes == 29, "transfer_no_recharge");
            Check(reader.GetString(0) == "abcdefghij", "text_exact");
            Check(!reader.Read() && budget.UsedBytes == 1, "eof_releases_row");
            Check(reader.RequireComplete().Rows == 1, "receipt");
        }
        Check(budget.UsedBytes == 0, "close_baseline");
        var tooSmall = new InputBufferBudget(48);
        using (var reader = Reader(bytes, tooSmall))
        {
            Reject(() => _ = reader.HasRows);
            Check(tooSmall.UsedBytes == 0, "failed_lookahead_release");
            Reject(() => reader.RequireComplete());
            Reject(() => reader.Read());
        }
        var noScratch = new InputBufferBudget(17);
        using var observed = new MemoryStream(bytes);
        using var decoder = new NativeRowDecoder(observed, Contract(TextColumn(), bytes, 1), default, noScratch);
        Reject(() => decoder.ReadNextLease());
        Check(observed.Position == 8 && noScratch.UsedBytes == 0, "scratch_denied_before_payload_read");
    }
    private static void Lifetimes()
    {
        byte[] bytes = Text("abcdefghij");
        var budget = new InputBufferBudget(49);
        using (var decoder = new NativeRowDecoder(new MemoryStream(bytes), Contract(TextColumn(), bytes, 1), default, budget))
        {
            Reject(() => decoder.ReadNext());
            using var row = decoder.ReadNextLease()!;
            Check(row.RetainedBytes == 28 && budget.UsedBytes == 29, "lease_retains");
            row.Dispose(); row.Dispose();
            Check(budget.UsedBytes == 1, "lease_release_once");
            Reject(() => _ = row.Values);
            Check(decoder.ReadNextLease() is null && decoder.RequireComplete().Rows == 1, "lease_eof");
        }
        Check(budget.UsedBytes == 0, "decoder_release");
        foreach (bool asynchronous in new[] { false, true })
        {
            using var cancel = new CancellationTokenSource();
            using var reader = Reader(bytes, budget, cancel.Token);
            Check(reader.HasRows, "cancel_prefetch");
            cancel.Cancel();
            try
            {
                if (asynchronous) reader.ReadAsync(default).GetAwaiter().GetResult(); else reader.Read();
                throw new Exception("cancel_not_seen");
            }
            catch (OperationCanceledException) { }
            Check(budget.UsedBytes == 0, "cancel_release");
            Reject(() => reader.RequireComplete());
        }
        using (var reader = Reader(bytes, budget)) { Check(reader.HasRows, "close_prefetch"); reader.Close(); }
        Check(budget.UsedBytes == 0, "early_close_release");
        byte[] pair = bytes.Concat(bytes).ToArray();
        using (var reader = Reader(pair, budget, default, 2))
            Check(reader.Read() && reader.Read() && !reader.Read(), "prior_row_released_before_decode");
        Check(budget.UsedBytes == 0, "two_row_release");
    }
    private static void FailureControls()
    {
        byte[] bytes = Text("abc");
        foreach (byte[] invalid in new[] { bytes[..^1], BitConverter.GetBytes(long.MaxValue), Text("\ud800") })
        {
            var budget = new InputBufferBudget(100);
            using var reader = Reader(invalid, budget);
            Reject(() => reader.Read());
            Check(budget.UsedBytes == 0, "decode_failure_release");
            Reject(() => reader.RequireComplete());
        }
        var emptyBudget = new InputBufferBudget(1);
        using (var reader = Reader(Array.Empty<byte>(), emptyBudget, default, 0))
            Check(!reader.Read() && reader.RequireComplete().Bytes == 0, "empty_no_row_reservation");
        Check(emptyBudget.UsedBytes == 0, "empty_close");
        var hugeMaximum = new InputBufferBudget(25);
        byte[] scalar = BitConverter.GetBytes(42L);
        using (var reader = new NativeInputReader(new MemoryStream(scalar), Contract(Bigint(), scalar, 1), default, hugeMaximum))
            Check(reader.Read() && reader.GetInt64(0) == 42 && !reader.Read(), "large_maximum_small_actual");
        Check(hugeMaximum.UsedBytes == 0, "scalar_close");
    }
    private static void NullableAndWide()
    {
        var nullable = NativeColumn.Admit("n", "bigint nullable", true, "bigint", 1, 8, null, null, null);
        byte[] nil = new byte[] { 255 };
        var budget = new InputBufferBudget(10);
        using (var reader = new NativeInputReader(new MemoryStream(nil), Contract(nullable, nil, 1), default, budget))
            Check(reader.Read() && reader.IsDBNull(0) && budget.UsedBytes == 9 && !reader.Read(), "null_slot_only");
        var emptyText = new InputBufferBudget(17);
        using (var reader = Reader(Text(""), emptyText))
            Check(reader.Read() && reader.GetString(0) == "" && emptyText.UsedBytes == 9, "empty_text_prefix_peak");
        byte[] combined = BitConverter.GetBytes(7L).Concat(Text("abcdefghij")).ToArray();
        var two = new NativeInputContract(new[] { Bigint(), TextColumn() }, Expected(combined, 1), new NativeInputLimits(int.MaxValue));
        foreach (long limit in new[] { 65L, 64L })
        {
            var shared = new InputBufferBudget(limit);
            using var reader = new NativeInputReader(new MemoryStream(combined), two, default, shared);
            if (limit == 65) Check(reader.Read() && shared.UsedBytes == 45, "prior_columns_peak");
            else { Reject(() => reader.Read()); Check(shared.UsedBytes == 0, "prior_columns_release"); }
        }
        var slots = Enumerable.Range(0, 100).Select(i => NativeColumn.Admit("n" + i, "bigint nullable", true, "bigint", 1, 8, null, null, null)).ToArray();
        byte[] allNull = Enumerable.Repeat((byte)255, 100).ToArray();
        var wide = new InputBufferBudget(802);
        using (var reader = new NativeInputReader(new MemoryStream(allNull), new NativeInputContract(slots, Expected(allNull, 1), new NativeInputLimits(100)), default, wide))
            Check(reader.Read() && wide.UsedBytes == 801 && !reader.Read(), "wide_slots");
        Check(wide.UsedBytes == 0, "wide_close");
    }
    private static void SharedOwnership()
    {
        byte[] bytes = Text("abcdefghij");
        var budget = new InputBufferBudget(49);
        using (var occupied = budget.Reserve(49))
        using (var input = new MemoryStream(bytes))
        {
            Reject(() => new NativeRowDecoder(input, Contract(TextColumn(), bytes, 1), default, budget));
            Check(budget.UsedBytes == 49 && input.CanRead && input.Position == 0, "constructor_denial_ownership");
        }
        var decoder = new NativeRowDecoder(new MemoryStream(bytes), Contract(TextColumn(), bytes, 1), default, budget);
        var lease = decoder.ReadNextLease()!;
        object[] borrowed = lease.Values;
        decoder.Dispose();
        Check(budget.UsedBytes == 28 && (string)lease.Values[0] == "abcdefghij", "external_lease_survives_decoder");
        lease.Dispose();
        Check(budget.UsedBytes == 0 && borrowed[0] is null, "borrowed_slots_cleared");
        using (var held = budget.Reserve(1))
        using (var reader = Reader(bytes, budget))
        {
            Reject(() => reader.Read());
            Check(budget.UsedBytes == 1, "failure_preserves_other_owner");
        }
        using (var legacy = new NativeRowDecoder(new MemoryStream(bytes), Contract(TextColumn(), bytes, 1)))
        {
            object[] values = legacy.ReadNext()!;
            legacy.Dispose();
            Check((string)values[0] == "abcdefghij", "legacy_array_transferred");
        }
        var encoded = new NativeInputContract(new[] { TextColumn() }, Expected(bytes, 1), new NativeInputLimits(8));
        using (var reader = new NativeInputReader(new MemoryStream(bytes), encoded, default, budget))
        {
            try { reader.Read(); throw new Exception("encoded_bound_missing"); }
            catch (InvalidDataException e) { Check(e.Message == "input.allocation_bound", "encoded_not_decoded_failure"); }
        }
        Check(budget.UsedBytes == 0, "encoded_failure_release");
    }
    private static NativeInputReader Reader(byte[] bytes, InputBufferBudget budget, CancellationToken token = default, long rows = 1)
        => new(new MemoryStream(bytes), Contract(TextColumn(), bytes, rows), token, budget);
    private static NativeInputContract Contract(NativeColumn c, byte[] bytes, long rows)
        => new(new[] { c }, Expected(bytes, rows), new NativeInputLimits(int.MaxValue));
    private static ExpectedInput Expected(byte[] bytes, long rows) => new(rows, bytes.LongLength, Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant());
    private static NativeColumn TextColumn() => NativeColumn.Admit("text", "nvarchar(max)", false, "nvarchar", 8, null, null, null, "utf-16le");
    private static NativeColumn Bigint() => NativeColumn.Admit("id", "bigint", false, "bigint", 0, 8, null, null, null);
    private static byte[] Text(string value)
    {
        byte[] data = new byte[value.Length * 2];
        for (int i = 0; i < value.Length; i++) { data[i * 2] = (byte)value[i]; data[i * 2 + 1] = (byte)(value[i] >> 8); }
        return BitConverter.GetBytes((long)data.Length).Concat(data).ToArray();
    }
    private static void Check(bool condition, string code) { if (!condition) throw new Exception(code); }
    private static void Reject(Action action)
    {
        try { action(); }
        catch (InvalidDataException) { return; }
        catch (InvalidOperationException) { return; }
        throw new Exception("expected_rejection");
    }
}
