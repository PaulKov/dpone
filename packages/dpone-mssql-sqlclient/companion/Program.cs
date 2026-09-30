using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

namespace Dpone.Mssql.SqlClient;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        WriteRequest? request = null;
        try
        {
            if (args is ["--self-test-application-name"])
            {
                Console.Out.Write(Protocol.ApplicationNameSelfTest());
                return 0;
            }
            int secretFd = ParseSecretDescriptor(args);
            Stopwatch deadline = Stopwatch.StartNew();
            request = Protocol.ReadRequest(Console.OpenStandardInput());
            await using FileStream secretStream = new(
                new SafeFileHandle((IntPtr)secretFd, ownsHandle: true), FileAccess.Read, 4096, isAsync: false);
            SqlCredentials credentials = Protocol.ReadCredentials(secretStream);
            VerifyArtifact(request);
            using CancellationTokenSource cancellation = new(request.DeadlineBudgetMs);
            WriteTimings result = await TargetWriter.WriteAsync(request, credentials, deadline, cancellation.Token);
            VerifyArtifact(request);
            Protocol.WriteResult(Console.OpenStandardOutput(), request, "success", result.Rows, WriterIdentity,
                result.LaunchSeconds, result.WriteSeconds, result.DisposeSeconds);
            return 0;
        }
        catch (OperationCanceledException) when (request is not null)
        {
            Protocol.WriteResult(Console.OpenStandardOutput(), request, "timeout", null, WriterIdentity, null, null, null);
            return 0;
        }
        catch (TimeoutException) when (request is not null)
        {
            Protocol.WriteResult(Console.OpenStandardOutput(), request, "timeout", null, WriterIdentity, null, null, null);
            return 0;
        }
        catch (TargetWriteException exception) when (request is not null)
        {
            Console.Error.Write(exception.Code);
            Protocol.WriteResult(Console.OpenStandardOutput(), request, "failure", null, WriterIdentity, null, null, null);
            return 0;
        }
        catch (Exception) when (request is not null)
        {
            Console.Error.Write("mssql_sqlclient.write_failed");
            Protocol.WriteResult(Console.OpenStandardOutput(), request, "failure", null, WriterIdentity, null, null, null);
            return 0;
        }
        catch (CompanionProtocolException exception)
        {
            Console.Error.Write(exception.Code);
            return 2;
        }
        catch (Exception)
        {
            Console.Error.Write("mssql_sqlclient.companion_failed");
            return 2;
        }
    }

    private static int ParseSecretDescriptor(string[] args)
    {
        if (args.Length != 2 || args[0] != "--secret-fd" || !int.TryParse(args[1], out int descriptor) || descriptor < 3)
            throw new InvalidDataException();
        return descriptor;
    }

    private static void VerifyArtifact(WriteRequest request)
    {
        FileInfo file = new(request.FilePath);
        if (!file.Exists || file.Length != request.EncodedBytes)
            throw new InvalidDataException();
        using FileStream stream = file.Open(FileMode.Open, FileAccess.Read, FileShare.Read);
        string digest = Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
        if (!string.Equals(digest, request.FileSha256, StringComparison.Ordinal))
            throw new InvalidDataException();
    }

    private static string WriterIdentity => Assembly.GetExecutingAssembly()
        .GetCustomAttributes<AssemblyMetadataAttribute>()
        .Single(attribute => attribute.Key == "WriterIdentity").Value!;
}
