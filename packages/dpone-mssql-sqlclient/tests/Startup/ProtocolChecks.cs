using System.Text;
using Dpone.SqlClient.Startup;

internal static class ProtocolChecks
{
    internal static void Run(string fixtures)
    {
        byte[] body = File.ReadAllBytes(Path.Combine(fixtures, "launch-v1.json"));
        StartupLaunch launch = StartupLaunch.Parse(body);
        Check(launch.Digest == File.ReadAllText(Path.Combine(fixtures, "launch-v1.sha256")).Trim(), "golden_digest");
        Check(launch.Process.Pid == 123 && launch.Descriptors.Input == 8, "golden_fields");
        Check(launch.AttemptSha256 == new string('a', 64) && launch.InputBindingSha256 == new string('c', 64), "session_bindings");
        string text = Encoding.UTF8.GetString(body);
        foreach (string bad in new[] {
            text.Replace("\"schema_version\":1", "\"schema_version\":true"),
            text.Replace("\"schema_version\":1", "\"schema_version\":1.0"),
            text.Replace("\"schema_version\":1", "\"schema_version\":1,\"schema_version\":1"),
            text.Replace("\"pid\":123", "\"pid\":123,\"extra\":0"),
            text.Replace("\"input\":8", "\"input\":3"),
            text.Replace("\"parent_pid\":122", "\"parent_pid\":123"),
            text.Replace("8589934592", "8589934591"),
            text.Replace("\"startup_deadline_ns\":10000000000", "\"startup_deadline_ns\":9223372036854775808")
        }) Reject(() => StartupLaunch.Parse(Encoding.UTF8.GetBytes(bad)));
        Reject(() => StartupLaunch.Parse(Encoding.UTF8.GetBytes(text.Replace("11111111-1111-4111-8111-111111111111", Guid.Empty.ToString("D")))));
        Reject(() => StartupLaunch.Parse(Encoding.UTF8.GetBytes(text.Replace("22222222-2222-4222-8222-222222222222", Guid.Empty.ToString("D")))));
        Reject(() => StartupLaunch.Parse(new byte[] { 255 }));
        Reject(() => StartupLaunch.Parse(new byte[16385]));
        foreach (string integer in new[] { "-9223372036854775808", "0", "9223372036854775807", "9223372036854775808", "18446744073709551615" })
        {
            using var numeric = JsonWire.Document(Encoding.UTF8.GetBytes("{\"value\":" + integer + "}"));
            Check(Encoding.UTF8.GetString(JsonWire.Canonical(numeric.RootElement)) == "{\"value\":" + integer + "}", "canonical_uint64");
        }
        foreach (string invalid in new[] { "18446744073709551616", "-9223372036854775809", "1.0", "1e0", "1E1" })
            Reject(() =>
            {
                using var numeric = JsonWire.Document(Encoding.UTF8.GetBytes("{\"value\":" + invalid + "}"));
                JsonWire.Canonical(numeric.RootElement);
            });
        Console.WriteLine("PASS startup golden/strict protocol checks and unsigned canonical boundaries");
    }
    private static void Check(bool value, string code) { if (!value) throw new Exception(code); }
    private static void Reject(Action action)
    {
        try { action(); } catch (InvalidDataException) { return; }
        throw new Exception("expected_protocol_rejection");
    }
}
