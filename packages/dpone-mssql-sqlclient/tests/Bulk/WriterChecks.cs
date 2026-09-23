using System.Diagnostics;
using System.Security.Cryptography;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Input;
using Microsoft.Data.SqlClient;

internal static class WriterChecks
{
    internal static void Run()
    {
        using var connection = new SqlConnection("Server=unused;Pooling=false;Enlist=false;MultipleActiveResultSets=false;ConnectRetryCount=0");
        using var input = new MemoryStream();
        var contract = new NativeInputContract(new[] { NativeColumn.Admit("value", "bigint", false, "bigint", 0, 8, null, null, null) },
            new ExpectedInput(0, 0, Convert.ToHexString(SHA256.HashData(Array.Empty<byte>())).ToLowerInvariant()), new NativeInputLimits(100));
        using var reader = new NativeInputReader(input, contract);
        ExistingConnectionBulkWriter Create() => new(connection, reader, contract, new BulkDestination("database", "schema", "stage"),
            new BulkInputPolicy(3, 1048576), new BulkDeadline(TimeProvider.System, Stopwatch.GetTimestamp() + 30 * Stopwatch.Frequency), reader.RequireComplete);
        var writer = Create();
        int retained = 0;
        var outcome = writer.WriteAsync(value => { if(value.Code != "copy_failed" || value.Input is not null) throw new Exception("failure_outcome"); retained++; }).GetAwaiter().GetResult();
        if (retained != 1 || outcome.Code != "copy_failed" || !input.CanRead) throw new Exception("retained_failure");
        try { writer.WriteAsync(_ => retained++).GetAwaiter().GetResult(); throw new Exception("repeated_write"); }
        catch (InvalidOperationException) { }
        if (retained != 1) throw new Exception("repeated_retention");
        var marker = new ApplicationException("retention_failure");
        try { Create().WriteAsync(_ => throw marker).GetAwaiter().GetResult(); throw new Exception("retention_promoted"); }
        catch (ApplicationException caught) when (ReferenceEquals(marker, caught)) { }
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        outcome = Create().WriteAsync(_ => { }, cancelled.Token).GetAwaiter().GetResult();
        if (outcome.Code != "copy_cancelled" || input.Position != 0) throw new Exception("pre_cancelled_writer");
        Console.WriteLine("PASS one-shot failure retention, original callback failure, caller-owned input and cancellation before SQL");
    }
}
