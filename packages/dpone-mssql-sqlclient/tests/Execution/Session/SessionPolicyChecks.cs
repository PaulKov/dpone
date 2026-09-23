using System.Data;
using Dpone.SqlClient.Execution.Session;
using Microsoft.Data.SqlClient;
using static SessionTestData;

internal static class SessionPolicyChecks
{
    internal static void Run()
    {
        int attempts = 0;
        var none = SqlConfigurableRetryFactory.CreateNoneRetryProvider();
        try { none.Execute<int>(new object(), () => { attempts++; throw new InvalidOperationException("synthetic_failure"); }); }
        catch (InvalidOperationException) { }
        Check(attempts == 1);
        using (var closed = new SqlConnection { RetryLogicProvider = none })
            Check(ReferenceEquals(closed.RetryLogicProvider, none));
        foreach (bool allow in new[] { false, true })
        {
            var policy = WriterConnectionPolicy.Build(Credentials(), allow, 3);
            Check(!policy.Pooling && !policy.MultipleActiveResultSets && !policy.Enlist && policy.ConnectRetryCount == 0);
            Check(!policy.IntegratedSecurity && !policy.PersistSecurityInfo && policy.Authentication == SqlAuthenticationMethod.SqlPassword);
            Check(policy.Encrypt.Equals(SqlConnectionEncryptOption.Mandatory) && !policy.TrustServerCertificate && policy.ConnectTimeout == 3);
            Check(policy.DataSource == "tcp:synthetic.invalid,1433" && policy.InitialCatalog == "synthetic_db");
            Check(policy.UserID == "synthetic_writer" && policy.Password == "synthetic-only-password");
        }
        Reject(() => WriterConnectionPolicy.Build(Credentials("disposable_test"), false, 3));
        Check(WriterConnectionPolicy.Build(Credentials("disposable_test"), true, 3).TrustServerCertificate);
        Reject(() => WriterConnectionPolicy.Build(Credentials(), false, 0));
        Reject(() => WriterConnectionPolicy.Build(null!, false, 3));
        string[] projection = WriterSessionSql.Self.Split("FROM", StringSplitOptions.None)[0]["SELECT ".Length..]
            .Split(',').Select(value => value.Trim()).ToArray();
        foreach ((int slot, string expression) in new[] { (0, "CAST(@@SPID AS int)"), (2, "CAST(DB_ID() AS int)"),
            (8, "CAST(USER_ID() AS int)"), (12, "CAST(@@TRANCOUNT AS int)"), (13, "CAST(XACT_STATE() AS int)"), (14, "CAST((@@OPTIONS & 2) AS int)") })
            Check(projection[slot] == expression);
        Check(WriterSessionSql.Self.Contains("s.context_info", StringComparison.Ordinal));
        Check(WriterSessionSql.Self.Contains("s.login_time", StringComparison.Ordinal));
        Check(!WriterSessionSql.Self.Contains("dm_exec_connections", StringComparison.Ordinal));
        Check(!WriterSessionSql.Self.Contains("CONTEXT_INFO()", StringComparison.Ordinal));
        foreach (int rows in new[] { 0, 1, 2 })
        {
            using var reader = Table(rows).CreateDataReader();
            if (rows == 1) Check(WriterSessionSql.ReadOne(reader, ClientId, default).GetAwaiter().GetResult().Nonce == Nonce);
            else Reject(() => WriterSessionSql.ReadOne(reader, ClientId, default).GetAwaiter().GetResult());
        }
        using (var data = new DataSet())
        {
            data.Tables.Add(Table(1)); data.Tables.Add(Table(0));
            using var reader = data.CreateDataReader();
            Reject(() => WriterSessionSql.ReadOne(reader, ClientId, default).GetAwaiter().GetResult());
        }
        using (var shortTable = new DataTable())
        {
            shortTable.Columns.Add("bad", typeof(int)); shortTable.Rows.Add(1);
            using var reader = shortTable.CreateDataReader();
            Reject(() => WriterSessionSql.ReadOne(reader, ClientId, default).GetAwaiter().GetResult());
        }
        Console.WriteLine("PASS concrete fixed connection policy, explicit TLS exception and SQL row/result cardinality");
    }
    private static DataTable Table(int rows)
    {
        var table = new DataTable();
        foreach (object value in Row()) table.Columns.Add("v" + table.Columns.Count, value.GetType());
        for (int i = 0; i < rows; i++) table.Rows.Add(Row());
        return table;
    }
}
