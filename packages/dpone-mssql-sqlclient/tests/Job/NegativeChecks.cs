using System.Text;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Startup;
using Dpone.SqlClient.Session;

internal static class NegativeChecks
{
    private static int count;
    private static void Reject(Action action)
    {
        try { action(); }
        catch (InvalidDataException error)
        {
            if (error.Message != "mssql_native.sqlclient_job_invalid") throw new Exception("job.error_not_redacted");
            count++; return;
        }
        throw new Exception("job.negative_accepted");
    }
    private static byte[] Bytes(JsonNode value) => Encoding.UTF8.GetBytes(value.ToJsonString());
    internal static void Run(byte[] body, StartupLaunch launch, byte[] launchBody)
    {
        var original = JsonNode.Parse(body)!;
        var job = SqlClientJob.Parse(body);
        void Mutate(Action<JsonNode> mutation)
        {
            var changed = original.DeepClone(); mutation(changed); Reject(() => SqlClientJob.Parse(Bytes(changed)));
        }
        Reject(() => SqlClientJob.Parse(Array.Empty<byte>()));
        Reject(() => SqlClientJob.Parse(null!));
        Reject(() => SqlClientJob.Parse(new byte[SqlClientJob.MaxJobBytes + 1]));
        Reject(() => SqlClientJob.Parse(Encoding.UTF8.GetBytes("{\"schema_version\":1," + Encoding.UTF8.GetString(body)[1..])));
        foreach (string path in new[] { "", "identity", "ownership", "object_identity", "input", "input.expected", "input.file_identity", "input.columns.0" }.Concat(job.Credentials is null ? Array.Empty<string>() : new[] { "credentials" }))
        {
            JsonObject At(JsonNode value)
            {
                foreach (string segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries)) value = int.TryParse(segment, out int index) ? value[index]! : value[segment]!;
                return value.AsObject();
            }
            Mutate(v => At(v)["unexpected"] = 1);
            Mutate(v => At(v).Remove(At(v).First().Key));
            var target = At(original);
            string nested = target.ToJsonString();
            string duplicate = "{" + System.Text.Json.JsonSerializer.Serialize(target.First().Key) + ":null," + nested[1..];
            string serialized = original.ToJsonString().Replace(nested, duplicate, StringComparison.Ordinal);
            Reject(() => SqlClientJob.Parse(Encoding.UTF8.GetBytes(serialized)));
        }
        foreach (JsonNode? alias in new JsonNode?[] { JsonValue.Create(true), JsonValue.Create(1.5), JsonValue.Create("1"), null })
        {
            Mutate(v => v["batch_rows"] = alias?.DeepClone());
            Mutate(v => v["input"]!["file_identity"]!["device"] = alias?.DeepClone());
            Mutate(v => v["input"]!["expected"]!["rows"] = alias?.DeepClone());
        }
        foreach (string number in new[] { "1.0", "1e0", "18446744073709551616", "-1" })
            Mutate(v => v["input"]!["file_identity"]!["device"] = JsonNode.Parse(number));
        Mutate(v => v["input"]!["expected"]!["rows"] = JsonNode.Parse("9223372036854775808"));
        Mutate(v => v["input"]!["columns"]![0]!["target_type"] = "String");
        Mutate(v => v["input"]!["columns"]![0]!["source_type"] = "int");
        Mutate(v => v["input"]!["columns"]![0]!["nullable"] = 1);
        Mutate(v => v["input"]!["columns"]![0]!["prefix_width"] = 8);
        Mutate(v => v["input"]!["columns"]![0]!["scale"] = 7);
        Mutate(v => v["input"]!["columns"]![0]!["name"] = new string('a', 129));
        Mutate(v => v["identity"]!["database"] = string.Concat(Enumerable.Repeat("😀", 65)));
        Mutate(v => v["input"]!["columns"]![1]!["name"] = v["input"]!["columns"]![0]!["name"]!.DeepClone());
        Mutate(v => v["input"]!["file_identity"]!["size"] = 999);
        Mutate(v => v["identity"]!["file_sha256"] = new string('f', 64));
        Mutate(v => v["input"]!["columns"] = new JsonArray());
        Mutate(v => v["input"]!["fd"] = 2);
        string text = Encoding.UTF8.GetString(body);
        Reject(() => SqlClientJob.Parse(Encoding.UTF8.GetBytes(text.Replace("\"input_mode\":\"" + job.InputMode + "\"", "\"input_mode\":\"\\ud800\""))));
        if (job.Credentials is not null)
        {
            Mutate(v => v["credentials"] = null);
            Mutate(v => v["credentials"]!["database"] = "different");
            Mutate(v => v["credentials"]!["tls_profile"] = "default");
            Mutate(v => v["credentials"]!["host"] = "host;password=secret");
            Mutate(v => v["credentials"]!["password"] = "");
            Mutate(v => v["credentials"]!["password"] = new string('a', 16385));
            Mutate(v => v["session_nonce"] = null);
            Mutate(v => v["session_nonce"] = new string('0', 64));
            var different = original.DeepClone(); different["credentials"]!["password"] = "another-synthetic-password";
            different["credentials"]!["username"] = "another-synthetic-user";
            if (SqlClientJob.Parse(Bytes(different)).BindingDigest != job.BindingDigest) throw new Exception("job.secret_in_digest");
            different["credentials"]!["tls_profile"] = "disposable_test";
            var disposable = SqlClientJob.Parse(Bytes(different));
            var policy = new JobTransportPolicy("mssql_sqlclient", job.InputMode, job.BatchRows, job.MaxInputBatchBytes, launch.AddressSpaceBytes);
            Reject(() => disposable.Validate(launch, job.Ownership, job.ObjectIdentity, policy, job.SessionNonce, "disposable_test", false, 0));
            disposable.Validate(launch, job.Ownership, job.ObjectIdentity, policy, job.SessionNonce, "disposable_test", true, 0);
            if (job.Credentials.ToString() != "SqlClientCredentials [REDACTED]") throw new Exception("job.credential_diagnostic");
        }
        else
        {
            Mutate(v => v["session_nonce"] = new string('a', 64));
            Mutate(v => v["input"]!["expected"]!["rows"] = 1);
        }
        if (job.ToString() != "SqlClientJob [REDACTED]") throw new Exception("job.diagnostic");
        Bindings(job, launch, original, launchBody);
        Console.WriteLine("PASS redacted rejection controls cumulative=" + count);
    }
    private static void Bindings(SqlClientJob job, StartupLaunch launch, JsonNode original, byte[] launchBody)
    {
        var policy = new JobTransportPolicy("mssql_sqlclient", job.InputMode, job.BatchRows, job.MaxInputBatchBytes, launch.AddressSpaceBytes);
        void Validate(SqlClientJob value, JobTransportPolicy? p = null, long now = 0) => value.Validate(launch, job.Ownership, job.ObjectIdentity, p ?? policy, job.SessionNonce, job.Credentials?.TlsProfile, false, now);
        foreach (var p in new[] { policy with { Backend = "bcp" }, policy with { Input = "other" }, policy with { BatchRows = policy.BatchRows + 1 }, policy with { MaxInputBatchBytes = policy.MaxInputBatchBytes + 1 }, policy with { AddressSpaceBytes = policy.AddressSpaceBytes + 1 } }) Reject(() => Validate(job, p));
        Reject(() => Validate(job, now: -1)); Reject(() => Validate(job, now: launch.OperationDeadlineNs));
        Validate(job, now: launch.OperationDeadlineNs - 1);
        foreach (string field in new[] { "launch_sha256", "identity", "ownership", "object_identity", "input" })
        {
            var v = original.DeepClone();
            switch (field)
            {
                case "launch_sha256": v[field] = new string('f', 64); break;
                case "identity": v[field]!["policy_sha256"] = new string('f', 64); break;
                case "ownership": v[field]!["fence"] = job.Ownership.Fence == 1 ? 2 : 1; break;
                case "object_identity": v[field]!["object_id"] = job.ObjectIdentity.ObjectId == 1 ? 2 : 1; break;
                case "input": v[field]!["fd"] = job.Input.Fd + 1; break;
            }
            var altered = SqlClientJob.Parse(Bytes(v)); Reject(() => Validate(altered));
        }
        foreach (string field in new[] { "target_key", "run_id", "ordinal", "attempt", "plan_sha256", "policy_sha256", "implementation_sha256", "schema", "table", "owner_binding" })
        {
            var v = original.DeepClone();
            v["identity"]![field] = field is "ordinal" or "attempt" ? JsonValue.Create(v["identity"]![field]!.GetValue<long>() == 0 ? 1 : 0)
                : JsonValue.Create(field.EndsWith("sha256", StringComparison.Ordinal) || field == "owner_binding" ? new string('9', 64) : "different");
            Reject(() => Validate(SqlClientJob.Parse(Bytes(v))));
        }
        // Change the descriptor and rebind its launch digest: only the original launch FD role now disagrees.
        var fdJob = original.DeepClone(); fdJob["input"]!["fd"] = job.Input.Fd + 1;
        var changedDescriptor = SqlClientJob.Parse(Bytes(fdJob)).Input;
        // The original launch bytes are supplied by the test fixture, never reconstructed from the Job.
        var launchNode = JsonNode.Parse(launchBody)!;
        launchNode["input_binding_sha256"] = changedDescriptor.Digest;
        var fdLaunch = StartupLaunch.Parse(Bytes(launchNode)); fdJob["launch_sha256"] = fdLaunch.Digest;
        var fdChanged = SqlClientJob.Parse(Bytes(fdJob));
        Reject(() => fdChanged.Validate(fdLaunch, job.Ownership, job.ObjectIdentity, policy, job.SessionNonce, job.Credentials?.TlsProfile, false, 0));
        Reject(() => job.Validate(launch, job.Ownership, job.ObjectIdentity, policy, "wrong", job.Credentials?.TlsProfile, false, 0));
        Reject(() => job.Validate(launch, job.Ownership, job.ObjectIdentity, policy, job.SessionNonce, "wrong", false, 0));
    }
}
