using Dpone.SqlClient.Job;
using Dpone.SqlClient.Startup;
using System.Text.Json;

foreach (string name in new[] { "rows", "arrow-unicode-u64", "empty" })
{
    string prefix = Path.Combine(args[0], name);
    byte[] body = File.ReadAllBytes(prefix + ".job.json");
    SqlClientJob job = SqlClientJob.Parse(body);
    using var expected = JsonDocument.Parse(File.ReadAllBytes(prefix + ".digests.json"));
    void Equal(bool condition) { if (!condition) throw new Exception("job.vector_mismatch"); }
    Equal(job.Encode().SequenceEqual(body));
    Equal(job.Input.Encode().SequenceEqual(File.ReadAllBytes(prefix + ".input.json")));
    byte[] launchBody = File.ReadAllBytes(prefix + ".launch.json");
    using var launchDoc = JsonWire.Document(launchBody);
    Equal(JsonWire.Canonical(launchDoc.RootElement).SequenceEqual(launchBody));
    var launch = StartupLaunch.Parse(launchBody);
    Equal(launch.Digest == expected.RootElement.GetProperty("launch_sha256").GetString());
    Equal(job.Identity.Digest == expected.RootElement.GetProperty("attempt_sha256").GetString());
    Equal(job.Input.Digest == expected.RootElement.GetProperty("input_sha256").GetString());
    Equal(job.BindingDigest == expected.RootElement.GetProperty("job_binding_sha256").GetString());
    Equal(job.BindingBytes().SequenceEqual(File.ReadAllBytes(prefix + ".job-binding.json")));
    job.Validate(launch, job.Ownership, job.ObjectIdentity,
        new("mssql_sqlclient", job.InputMode, job.BatchRows, job.MaxInputBatchBytes, launch.AddressSpaceBytes),
        job.SessionNonce, job.Credentials?.TlsProfile, false, 0);
    NegativeChecks.Run(body, launch, launchBody);
    Console.WriteLine("PASS job vectors and negative controls: " + name);
}
