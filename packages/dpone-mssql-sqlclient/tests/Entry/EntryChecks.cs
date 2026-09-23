using Dpone.SqlClient.Entry;
using System.Text;
using System.Reflection;
using Dpone.SqlClient.Startup;

internal static class EntryChecks
{
    private static async Task<int> Main(string[] args)
    {
        string[] Valid(string launch = "{}") => ["--launch", launch, "--companion-root", "/companion", "--runtime-root", "/runtime", "--deployment-manifest", "/manifest"];
        int count = 0;
        void Reject(string[] args)
        {
            try { Program.ParseArguments(args); }
            catch (Exception) { count++; return; }
            throw new Exception("entry.accepted_invalid_arguments");
        }
        foreach (int length in new[] { 0, 1, 2, 6, 7, 9, 10 }) Reject(Enumerable.Repeat("x", length).ToArray());
        foreach (string key in new[] { "--unknown", "--disposable-test", "--Launch", "--runtime-root" })
        { var invalid = Valid(); invalid[0] = key; Reject(invalid); }
        foreach (string value in new[] { "", new string('a', 16385), new string('é', 8193), "\ud800" }) Reject(Valid(value));
        foreach (string value in new[] { "{}", new string('a', 16384), new string('é', 8192) })
        {
            var parsed = Program.ParseArguments(Valid(value));
            if (!parsed.LaunchBody.SequenceEqual(Encoding.UTF8.GetBytes(value))) throw new Exception("entry.encoding");
            count++;
        }
        var reordered = Valid().Chunk(2).Reverse().SelectMany(pair => pair).ToArray();
        Program.ParseArguments(reordered); count++;
        if (await Program.Main([]) != 70 || await Program.Main(Valid("invalid-json")) != 70)
            throw new Exception("entry.failure_exit");
        count += 2;
        if (args.Length == 1)
        {
            string root = args[0];
            var flags = BindingFlags.NonPublic | BindingFlags.Static;
            typeof(DeploymentAdmission).GetMethod("Profile", flags)!.Invoke(null,
                [File.ReadAllBytes(Path.Combine(root, "Dpone.Mssql.SqlClient.Worker.runtimeconfig.json"))]);
            var inventory = Directory.GetFiles(root, "*", SearchOption.AllDirectories)
                .Select(path => Path.GetRelativePath(root, path)).ToHashSet(StringComparer.Ordinal);
            typeof(DeploymentAdmission).GetMethod("Dependencies", flags)!.Invoke(null,
                [File.ReadAllBytes(Path.Combine(root, "Dpone.Mssql.SqlClient.Worker.deps.json")), inventory]);
            count += 2;
        }
        else if (args.Length != 0) throw new Exception("checks.arguments");
        Console.WriteLine($"PASS {count} entry checks");
        return 0;
    }
}
