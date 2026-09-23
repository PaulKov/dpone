using System.Text.Json.Nodes;
using Dpone.SqlClient.Execution;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Startup;

if (args.Length == 1 && args[0] == "synthetic-live")
{
    Environment.ExitCode = await SyntheticLiveChecks.Run();
    return;
}

foreach (string name in new[] { "rows", "arrow-unicode-u64", "empty" })
{
    byte[] original = File.ReadAllBytes(Path.Combine(args[0], name + ".job.json"));
    var launch = StartupLaunch.Parse(File.ReadAllBytes(Path.Combine(args[0], name + ".launch.json")));
    WorkerJobAdmission.RequireDeclaration(SqlClientJob.Parse(original), launch, false, 0);
    Reject(() => WorkerJobAdmission.RequireDeclaration(SqlClientJob.Parse(original), launch, false, launch.OperationDeadlineNs));
    Reject(() => WorkerJobAdmission.RequireDeclaration(SqlClientJob.Parse(original), launch, false, -1));
    foreach (Action<JsonNode> change in new Action<JsonNode>[] {
        n => n["launch_sha256"] = new string('f', 64),
        n => n["identity"]!["target_key"] = "different",
        n => n["input"]!["fd"] = 98765 })
    {
        JsonNode node = JsonNode.Parse(original)!; change(node);
        var job = SqlClientJob.Parse(System.Text.Encoding.UTF8.GetBytes(node.ToJsonString()));
        Reject(() => WorkerJobAdmission.RequireDeclaration(job, launch, false, 0));
    }
}
Console.WriteLine("PASS worker declarations bind original launch/attempt/input/FD/deadline");
GrantChecks.Run(args[0]);
await WorkerPipeChecks.Run(args[0]);

static void Reject(Action action)
{
    try { action(); } catch (InvalidDataException) { return; }
    throw new Exception("test.expected_rejection");
}
