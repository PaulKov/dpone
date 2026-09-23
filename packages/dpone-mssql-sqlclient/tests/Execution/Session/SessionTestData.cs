using System.Text.Json;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Bulk;

internal static class SessionTestData
{
    internal static int Checks;
    internal static string Nonce => new('a', 64);
    internal static Guid ClientId => new("11111111-2222-4333-8444-555555555555");
    internal static void Check(bool value) { if (!value) throw new Exception("test.writer_check"); Checks++; }
    internal static void Reject(Action action)
    {
        try { action(); }
        catch (Exception error) when (error is InvalidDataException or InvalidOperationException)
        {
            Check(error.Message is "writer.session_invalid" or "writer.session_failed");
            Check(error.InnerException is null); return;
        }
        throw new Exception("test.expected_writer_rejection");
    }
    internal static SqlClientCredentials Credentials(string tls = "verified")
    {
        using var doc = JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(new { host = "synthetic.invalid", port = 1433,
            database = "synthetic_db", username = "synthetic_writer", password = "synthetic-only-password", tls_profile = tls }));
        return new(doc.RootElement);
    }
    internal static object[] Row() => new object[] { 71, Convert.FromHexString(Nonce), 5, "synthetic_db", "synthetic_writer", new byte[] { 1, 2 },
        "synthetic_writer", new byte[] { 1, 2 }, 11, "synthetic_principal", new byte[] { 8, 9 }, new DateTime(2026, 9, 15, 1, 2, 3, DateTimeKind.Unspecified), 0, 1, 0 };
    internal static BulkDeadline Deadline(TestClock? clock = null) => new(clock ?? new TestClock(), 10_000_000_000);
}
internal sealed class TestClock : TimeProvider
{
    internal long Now;
    public override long TimestampFrequency => 1_000_000_000;
    public override long GetTimestamp() => Now;
}
