using System.Security.Cryptography;
using System.Text.Json;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Execution.Session;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;

// Explicit optional root-owned local-synthetic execution only. Credentials enter
// stdin memory, never argv/env/files or diagnostics. Default mode performs no SQL.
internal static class SyntheticLiveChecks
{
    internal static async Task<int> Run()
    {
        string phase = "input";
        try
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            byte[] body = new byte[32769]; int count = 0;
            using (Stream input = Console.OpenStandardInput())
            {
                while (count < body.Length)
                {
                    int read = await input.ReadAsync(body.AsMemory(count), cancellation.Token);
                    if (read == 0) break;
                    count += read;
                }
            }
            if (count is < 1 or > 32768) throw new InvalidDataException();
            using var document = JsonWire.Document(body[..count]);
            var credentials = new SqlClientCredentials(document.RootElement);
            string nonce = Convert.ToHexString(RandomNumberGenerator.GetBytes(32)).ToLowerInvariant();
            var deadline = new BulkDeadline(new SessionClock(), checked(StartupClock.NowNs() + 60_000_000_000));
            phase = "open";
            using (var session = new SingleWriterSession(credentials, nonce, deadline, allowDisposableTest: true))
            {
                OwnSqlSession first = await session.OpenAsync(cancellation.Token);
                var connection = session.Connection;
                phase = "same";
                OwnSqlSession second = session.RequireSame(cancellation.Token);
                if (first != second || first.Nonce != nonce || first.ClientConnectionId == Guid.Empty ||
                    first.Principal != second.Principal || !ReferenceEquals(connection, session.Connection)) throw new InvalidDataException();
                phase = "dispose";
            }
            Console.WriteLine(JsonSerializer.Serialize(new { status = "PASS", same_connection = true, stable_own_session = true, stable_principal = true, cleanup = true }));
            return 0;
        }
        catch (Exception)
        {
            Console.WriteLine(JsonSerializer.Serialize(new { status = "FAIL", phase }));
            return 1;
        }
    }
}
