using System.Text;
using Dpone.SqlClient.Execution;
using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Entry;

// Fixed production composition. No test mode, credential handling or second lifecycle.
internal static class Program
{
    internal static async Task<int> Main(string[] args)
    {
        try
        {
            var arguments = ParseArguments(args);
            using StartupContext context = StartupBootstrap.Start(arguments.LaunchBody, arguments.Installation);
            return await new WorkerRun().RunAsync(context, allowDisposableTest: false);
        }
        catch (Exception) { return 70; }
    }

    internal static (byte[] LaunchBody, StartupInstallation Installation) ParseArguments(string[] args)
    {
        if (args.Length != 8) throw new InvalidDataException("entry.arguments");
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < args.Length; i += 2)
            if (!values.TryAdd(args[i], args[i + 1])) throw new InvalidDataException("entry.arguments");
        if (!values.Keys.ToHashSet().SetEquals(new[] { "--launch", "--companion-root", "--runtime-root", "--deployment-manifest" }))
            throw new InvalidDataException("entry.arguments");
        string launch = values["--launch"];
        // Bound before encoding allocation; reject invalid UTF-16 instead of replacement.
        var utf8 = new UTF8Encoding(false, true);
        if (launch.Length is < 1 or > 16384 || utf8.GetByteCount(launch) > 16384)
            throw new InvalidDataException("entry.launch_size");
        return (utf8.GetBytes(launch), new StartupInstallation(values["--companion-root"],
            values["--runtime-root"], values["--deployment-manifest"]));
    }
}
