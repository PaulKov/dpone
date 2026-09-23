using System.Security.Cryptography;
using System.Text;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Input;

internal static class BudgetChecks
{
    internal static void Run()
    {
        var policy = new BulkInputPolicy(100, 1048576);
        byte[] data = TextRows(100, 10000);
        var contract = Contract(data, 100, 100000);
        var budget = new InputBufferBudget(policy.MaxInputBatchBytes);
        using (var stream = new MemoryStream(data))
        using (var reader = new BoundedArrowReader(stream, contract, policy, budget))
        {
            int count = 0;
            while (reader.Read()) { if (reader.GetString(0).Length != 10000) throw new Exception("byte_fidelity"); count++; }
            if (count != 100 || reader.MaxObservedBatchRows >= 100 || reader.RequireComplete().Rows != 100)
                throw new Exception("byte_flush");
        }
        if (budget.UsedBytes != 0) throw new Exception("byte_flush_leak");
        // Configured encoded max may exceed decoded budget; actual fitting rows must still work.
        data = TextRows(2, 250000);
        contract = Contract(data, 2, 8000000);
        using (var stream = new MemoryStream(data))
        using (var reader = new BoundedArrowReader(stream, contract, policy, budget))
        {
            int count = 0;
            while (reader.Read()) count++;
            if (count != 2 || reader.RequireComplete().Rows != 2) throw new Exception("encoded_cap_substitution");
        }
        if (budget.UsedBytes != 0) throw new Exception("large_row_leak");
        data = TextRows(1, 600000);
        contract = Contract(data, 1, 8000000);
        foreach (bool arrow in new[] { false, true })
        {
            using var stream = new MemoryStream(data);
            using System.Data.Common.DbDataReader reader = arrow ? new BoundedArrowReader(stream, contract, policy, budget)
                : new NativeInputReader(stream, contract, default, budget);
            try { reader.Read(); throw new Exception("oversized_row_accepted"); }
            catch (InvalidDataException) { }
            if (stream.Position > 8) throw new Exception("payload_read_before_budget_admission");
            reader.Dispose();
            if (budget.UsedBytes != 0) throw new Exception("oversized_row_leak");
        }
        data = TextRows(2, 100);
        contract = Contract(data, 2, 10000);
        using (var cancellation = new CancellationTokenSource())
        using (var stream = new MemoryStream(data))
        using (var reader = new BoundedArrowReader(stream, contract, policy, budget, cancellation.Token))
        {
            if (!reader.HasRows) throw new Exception("prefetch_missing");
            cancellation.Cancel();
            try { reader.Read(); throw new Exception("prefetch_cancel_bypass"); }
            catch (OperationCanceledException) { }
            try { reader.RequireComplete(); throw new Exception("cancel_receipt"); }
            catch (InvalidDataException) { }
            if (budget.UsedBytes != 0) throw new Exception("cancel_leak");
        }
        Console.WriteLine("PASS byte-triggered flush, fitting rows with larger encoded cap, both-mode oversized preallocation rejection, cancellation and zero leaked reservations");
    }
    private static byte[] TextRows(int rows, int chars)
    {
        using var result = new MemoryStream();
        byte[] value = Encoding.Unicode.GetBytes(new string('x', chars));
        for (int i = 0; i < rows; i++) { result.Write(BitConverter.GetBytes((long)value.Length)); result.Write(value); }
        return result.ToArray();
    }
    private static NativeInputContract Contract(byte[] data, long rows, int maxRowBytes) => new(
        new[] { NativeColumn.Admit("arbitrary_text", "nvarchar(max)", false, "nvarchar", 8, null, null, null, "utf-16le") },
        new ExpectedInput(rows, data.LongLength, Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant()), new NativeInputLimits(maxRowBytes));
}
