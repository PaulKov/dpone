using Dpone.SqlClient.Bulk;
using Microsoft.Data.SqlClient;

internal static class BulkChecks
{
    static int Main(string[] args)
    {
        if (args.Length == 2 && args[0] == "--live") { LiveSqlChecks.Run(args[1]).GetAwaiter().GetResult(); return 0; }
        var expected = SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers |
            SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction;
        if (ExistingConnectionBulkWriter.RequiredOptions != expected) throw new Exception("bulk_options");
        var destination = new BulkDestination("database", "schema]name", "table");
        if (destination.QualifiedName != "[database].[schema]]name].[table]") throw new Exception("identifier_quoting");
        var time = new FixedTime();
        var deadline = new BulkDeadline(time, 10999);
        if (deadline.SdkTimeoutSeconds() != 10) throw new Exception("subordinate_timeout");
        time.Now = 10500;
        try { deadline.SdkTimeoutSeconds(); throw new Exception("deadline_renewal"); }
        catch (TimeoutException) { }
        using var closed = new SqlConnection("Server=unused;Pooling=false;Enlist=false;MultipleActiveResultSets=false;ConnectRetryCount=0");
        try { ExistingConnectionBulkWriter.ValidateConnection(closed); throw new Exception("closed_connection"); }
        catch (InvalidDataException) { }
        Console.WriteLine("PASS explicit options, identifiers, original deadline and closed connection rejection");
        WriterChecks.Run();
        BudgetChecks.Run();
        if (args.Length > 0) RepresentationChecks.Run(args[0]);
        return 0;
    }
    sealed class FixedTime : TimeProvider
    {
        public long Now;
        public override long TimestampFrequency => 1000;
        public override long GetTimestamp() => Now;
    }
}
