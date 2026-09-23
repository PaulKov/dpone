using System.Text;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Execution;
using Dpone.SqlClient.Execution.Session;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Startup;

internal static class GrantChecks
{
    internal static void Run(string fixtures)
    {
        var job = SqlClientJob.Parse(File.ReadAllBytes(Path.Combine(fixtures, "rows.job.json")));
        var launch = StartupLaunch.Parse(File.ReadAllBytes(Path.Combine(fixtures, "rows.launch.json")));
        object[] row = { 72, Convert.FromHexString(job.SessionNonce!), 5, job.Credentials!.Database,
            job.Credentials.Username, new byte[] { 1 }, job.Credentials.Username, new byte[] { 1 },
            7, "mapped_writer", new byte[] { 2 }, new DateTime(2026, 1, 2, 3, 4, 5), 0, 0, 0 };
        Guid diagnostic = Guid.Parse("88888888-8888-4888-8888-888888888888");
        OwnSqlSession current = OwnSqlSession.Decode(row, diagnostic);
        JsonNode grant = JsonNode.Parse(File.ReadAllBytes(Path.Combine(fixtures, "..", "bulk-grant-principal-v1.json")))!;
        JsonNode jobNode = JsonNode.Parse(File.ReadAllBytes(Path.Combine(fixtures, "rows.job.json")))!;
        JsonNode launchNode = JsonNode.Parse(File.ReadAllBytes(Path.Combine(fixtures, "rows.launch.json")))!;
        foreach (string key in new[] { "attempt_sha256", "build_sha256", "input_binding_sha256", "operation_deadline_ns", "process" })
            grant[key] = launchNode[key]!.DeepClone();
        foreach (string key in new[] { "ownership", "object_identity" }) grant[key] = jobNode[key]!.DeepClone();
        grant["launch_sha256"] = launch.Digest;
        grant["remote_session"]!["nonce"] = job.SessionNonce;
        grant["remote_session"]!["login_time"] = "2026-01-02T03:04:05.000000";
        grant["resolved_database_principal"]!["name"] = "mapped_writer";
        grant["resolved_database_principal"]!["sid"] = "02";
        byte[] Bytes(JsonNode node) => Encoding.UTF8.GetBytes(node.ToJsonString());
        var accepted = new WorkerGrantAdmission().Accept(Bytes(grant), launch, job, current, current, 0);
        // These are authenticated parent attestations, not facts visible to the child.
        if (accepted.RemoteSession.ConnectionId == diagnostic.ToString()) throw new Exception("test.diagnostic_conflated");
        int mutations = 0;
        foreach (Action<JsonNode> mutate in new Action<JsonNode>[] {
            n => n["launch_sha256"] = new string('1', 64),
            n => n["attempt_sha256"] = new string('1', 64),
            n => n["build_sha256"] = new string('1', 64),
            n => n["input_binding_sha256"] = new string('1', 64),
            n => n["process"]!["pid"] = 999,
            n => n["process"]!["start_ticks"] = 999,
            n => n["process"]!["host_sha256"] = new string('1', 64),
            n => n["process"]!["boot_id"] = "99999999-9999-4999-8999-999999999999",
            n => n["ownership"]!["owner"] = "other",
            n => n["ownership"]!["fence"] = 999,
            n => n["ownership"]!["supervisor_id"] = "99999999-9999-4999-8999-999999999999",
            n => n["object_identity"]!["object_id"] = 999,
            n => n["object_identity"]!["fingerprint"] = new string('1', 64),
            n => n["operation_deadline_ns"] = launch.OperationDeadlineNs + 1,
            n => n["remote_session"]!["session_id"] = 999,
            n => n["remote_session"]!["nonce"] = new string('1', 64),
            n => n["remote_session"]!["login_time"] = "2026-01-02T03:04:06.000000",
            n => n["resolved_database_principal"]!["principal_id"] = 999,
            n => n["resolved_database_principal"]!["name"] = "other",
            n => n["resolved_database_principal"]!["sid"] = "03" })
        {
            JsonNode changed = grant.DeepClone(); mutate(changed);
            var admission = new WorkerGrantAdmission();
            Reject(() => admission.Accept(Bytes(changed), launch, job, current, current, 0));
            Reject(() => admission.Accept(Bytes(grant), launch, job, current, current, 0));
            mutations++;
        }
        foreach ((int index, object value) in new (int, object)[] { (0, 73), (1, Enumerable.Repeat((byte)1, 32).ToArray()),
            (2, 6), (3, "other"), (4, "other"), (5, new byte[] { 3 }), (6, "other"), (7, new byte[] { 3 }),
            (8, 8), (9, "other"), (10, new byte[] { 3 }), (11, new DateTime(2026, 1, 3)) })
        {
            object[] changed = (object[])row.Clone(); changed[index] = value;
            OwnSqlSession drift = OwnSqlSession.Decode(changed, diagnostic);
            Reject(() => new WorkerGrantAdmission().Accept(Bytes(grant), launch, job, current, drift, 0));
            mutations++;
        }
        Reject(() => new WorkerGrantAdmission().Accept(Bytes(grant), launch, job, current, OwnSqlSession.Decode(row, Guid.NewGuid()), 0));
        Reject(() => new WorkerGrantAdmission().Accept(Bytes(grant), launch, job, current, current, launch.OperationDeadlineNs));
        var once = new WorkerGrantAdmission();
        _ = once.Accept(Bytes(grant), launch, job, current, current, 0);
        Reject(() => once.Accept(Bytes(grant), launch, job, current, current, 0));
        JsonNode attestation = grant.DeepClone();
        attestation["remote_session"]!["connection_id"] = "99999999-9999-4999-8999-999999999999";
        attestation["remote_session"]!["connect_time"] = "2026-01-01T00:00:00.000000";
        attestation["remote_session"]!["authority_sha256"] = new string('9', 64);
        attestation["writer_observation_sha256"] = new string('8', 64);
        var preserved = new WorkerGrantAdmission().Accept(Bytes(attestation), launch, job, current, current, 0);
        if (preserved.RemoteSession.ConnectionId != "99999999-9999-4999-8999-999999999999" ||
            preserved.RemoteSession.ConnectTime != "2026-01-01T00:00:00.000000" ||
            preserved.RemoteSession.AuthoritySha256 != new string('9', 64) || preserved.WriterObservationSha256 != new string('8', 64))
            throw new Exception("test.attestation_changed");
        Console.WriteLine($"PASS grant admission: {mutations} independent binding/context mutations, one-shot, deadline, opaque attestations");
    }
    private static void Reject(Action action)
    {
        try { action(); } catch (InvalidDataException) { return; }
        throw new Exception("test.expected_grant_rejection");
    }
}
