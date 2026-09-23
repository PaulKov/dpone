using System.Text;
using Dpone.SqlClient.Startup;

// Synthetic component entrypoint, not the root-owned production main or SQL flow.
internal static class ManagedEntry
{
    private static int Main(string[] args)
    {
        try
        {
            if (args.Length != 8) return 70;
            var values = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int i = 0; i < args.Length; i += 2) if (!values.TryAdd(args[i], args[i + 1])) return 70;
            if (!values.Keys.ToHashSet().SetEquals(new[] { "--launch", "--companion-root", "--runtime-root", "--deployment-manifest" })) return 70;
            using StartupContext context = StartupBootstrap.Start(Encoding.UTF8.GetBytes(values["--launch"]),
                new(values["--companion-root"], values["--runtime-root"], values["--deployment-manifest"]));
            using var credentials = context.Take(StartupChannel.Credentials);
            byte[] synthetic = StartupFrame.Read((int)credentials.DangerousGetHandle(), context.Launch.OperationDeadlineNs, 1048576);
            if (Encoding.ASCII.GetString(synthetic) != "synthetic-control") return 71;
            using (var session = context.Take(StartupChannel.Session))
                StartupFrame.Write((int)session.DangerousGetHandle(), Encoding.ASCII.GetBytes("synthetic-session"), context.Launch.OperationDeadlineNs);
            using (var grant = context.Take(StartupChannel.Grant))
                if (Encoding.ASCII.GetString(StartupFrame.Read((int)grant.DangerousGetHandle(), context.Launch.OperationDeadlineNs)) != "synthetic-grant") return 72;
            using (var result = context.Take(StartupChannel.Result))
                StartupFrame.Write((int)result.DangerousGetHandle(), Encoding.ASCII.GetBytes("synthetic-result"), context.Launch.OperationDeadlineNs, 262144);
            return 0;
        }
        catch (Exception) { return 70; }
    }
}
