using Dpone.SqlClient.Execution.Session;
using static SessionTestData;

internal static class SnapshotChecks
{
    internal static void Run()
    {
        var original = OwnSqlSession.Decode(Row(), ClientId);
        Check(original.Nonce == Nonce && original.SessionId == 71 && original.Principal.Sid == "0809");
        Check(original.LoginSid != original.Principal.Sid);
        Check(original.LoginTime.Kind == DateTimeKind.Unspecified && original.ClientConnectionId == ClientId);
        original.RequireCredentials("synthetic_db", "synthetic_writer");
        Check(original == OwnSqlSession.Decode(Row(), ClientId));
        Check(original.ToString() == "OwnSqlSession [REDACTED]");
        foreach (int slot in Enumerable.Range(0, 15))
        {
            var row = Row(); row[slot] = true; Reject(() => OwnSqlSession.Decode(row, ClientId));
        }
        Reject(() => OwnSqlSession.Decode(Row()[..14], ClientId));
        Reject(() => OwnSqlSession.Decode(Row().Append((object)0).ToArray(), ClientId));
        Reject(() => OwnSqlSession.Decode(Row(), Guid.Empty));
        foreach (int index in new[] { 0, 2, 8, 12, 13, 14 })
        {
            var row = Row(); row[index] = (short)1; Reject(() => OwnSqlSession.Decode(row, ClientId));
            row[index] = 1L; Reject(() => OwnSqlSession.Decode(row, ClientId));
            row[index] = 1.0; Reject(() => OwnSqlSession.Decode(row, ClientId));
        }
        foreach (object invalid in new object[] { new byte[128], new byte[32], new byte[31], new byte[33], "wrong", null! })
        { var row = Row(); row[1] = invalid; Reject(() => OwnSqlSession.Decode(row, ClientId)); }
        foreach (object empty in new object[] { DBNull.Value, Array.Empty<byte>() })
        { var row = Row(); row[1] = empty; Check(OwnSqlSession.Decode(row, ClientId).Nonce is null); }
        foreach (int index in new[] { 5, 7, 10 })
        foreach (byte[] sid in new[] { Array.Empty<byte>(), new byte[86] })
        { var row = Row(); row[index] = sid; Reject(() => OwnSqlSession.Decode(row, ClientId)); }
        foreach (int index in new[] { 3, 4, 6, 9 })
        foreach (string text in new[] { "", new string('x', 129), "bad\0", "\ud800" })
        { var row = Row(); row[index] = text; Reject(() => OwnSqlSession.Decode(row, ClientId)); }
        foreach ((int index, int invalid) in new[] { (0, 0), (0, 32768), (2, 0), (8, 0), (12, 1), (13, -1), (13, 2), (14, 2) })
        { var row = Row(); row[index] = invalid; Reject(() => OwnSqlSession.Decode(row, ClientId)); }
        foreach (DateTime invalid in new[] { DateTime.UtcNow, new DateTime(1900, 1, 1), new DateTime(2026, 1, 1).AddTicks(1) })
        { var row = Row(); row[11] = invalid; Reject(() => OwnSqlSession.Decode(row, ClientId)); }
        var autocommit = Row(); autocommit[13] = 0; Check(OwnSqlSession.Decode(autocommit, ClientId) == original);
        Reject(() => original.RequireCredentials("wrong", "synthetic_writer"));
        Reject(() => original.RequireCredentials("synthetic_db", "wrong"));
        for (int i = 0; i < 12; i++)
        {
            var row = Row();
            row[i] = row[i] switch { int n => n + 1, byte[] b => b.Select(v => (byte)(v + 1)).ToArray(),
                string s => s + "x", DateTime d => d.AddMilliseconds(1), _ => throw new Exception("test.type") };
            var changed = OwnSqlSession.Decode(row, ClientId);
            Check(changed != original);
            if (i != 1) Reject(() => changed.RequireSameIncarnation(original));
        }
        var newClient = OwnSqlSession.Decode(Row(), Guid.NewGuid()); Reject(() => newClient.RequireSameIncarnation(original));
        var impersonated = Row(); impersonated[7] = new byte[] { 3 }; Reject(() => OwnSqlSession.Decode(impersonated, ClientId).RequireCredentials("synthetic_db", "synthetic_writer"));
        Console.WriteLine("PASS own-session exact types, nonce/transaction/identity rejection and immutable continuity");
    }
}
