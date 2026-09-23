using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Session;

internal sealed class RawDescriptor : IDisposable
{
    [DllImport("libc", SetLastError = true)] internal static extern int open(string path, int flags);
    [DllImport("libc", SetLastError = true)] internal static extern int close(int fd);
    [DllImport("libc", SetLastError = true)] internal static extern int fcntl(int fd, int op);
    [DllImport("libc", SetLastError = true)] internal static extern long lseek(int fd, long offset, int origin);
    [DllImport("libc", SetLastError = true)] internal static extern int pipe(int[] descriptors);
    [DllImport("libc", SetLastError = true)] internal static extern int dup2(int source, int destination);
    [DllImport("libc", SetLastError = true)] internal static extern int chmod(string path, int mode);
    internal int Fd { get; private set; }
    internal RawDescriptor(string path, int flags = 0)
    {
        Fd = open(path, flags); if (Fd < 3) throw new Exception("test.open");
    }
    internal void Transferred() => Fd = -1;
    public void Dispose() { if (Fd >= 0) { close(Fd); Fd = -1; } }
}

internal sealed class TestClock : TimeProvider
{
    internal long Now;
    internal int Calls;
    internal int ExpireOn = int.MaxValue;
    public override long TimestampFrequency => 1_000_000_000;
    public override long GetTimestamp() => ++Calls >= ExpireOn ? 10_000_000_000 : Now;
}

internal static class SealedInputFixtures
{
    internal static int Checks;
    internal static void Assert(bool value) { if (!value) throw new Exception("test.assertion"); Checks++; }
    internal static void Reject<T>(Action action) where T : Exception
    {
        try { action(); } catch (T) { Checks++; return; }
        throw new Exception("test.rejection_missing_" + typeof(T).Name);
    }
    internal static BulkDeadline Deadline() => new(new SessionClock(), Dpone.SqlClient.Startup.StartupClock.NowNs() + 60_000_000_000);
    internal static JobFileIdentity Oracle(string path)
    {
        // Independent GNU stat fstat-derived values, not the production statx helper.
        var start = new ProcessStartInfo("/usr/bin/stat") { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add("--printf=%d|%i|%s|%Y|%y|%Z|%z"); start.ArgumentList.Add("--"); start.ArgumentList.Add(path);
        using var process = Process.Start(start)!;
        string[] fields = process.StandardOutput.ReadToEnd().Split('|'); process.WaitForExit();
        if (process.ExitCode != 0 || fields.Length != 7) throw new Exception("test.stat_oracle");
        long Timestamp(string seconds, string printed)
        {
            int point = printed.IndexOf('.');
            return checked(long.Parse(seconds, CultureInfo.InvariantCulture) * 1_000_000_000 + long.Parse(printed.Substring(point + 1, 9), CultureInfo.InvariantCulture));
        }
        return new(ulong.Parse(fields[0], CultureInfo.InvariantCulture), ulong.Parse(fields[1], CultureInfo.InvariantCulture),
            long.Parse(fields[2], CultureInfo.InvariantCulture), Timestamp(fields[3], fields[4]), Timestamp(fields[5], fields[6]));
    }
    internal static JobInputDescriptor Descriptor(int fd, byte[] body, JobFileIdentity stat)
    {
        var v = new { schema_version = 1, fd,
            columns = new[] { new { name = "synthetic", source_type = "bigint", target_type = "Int64", storage_type = "bigint",
                nullable = false, prefix_width = 0, fixed_length = (int?)8, precision = (int?)null, scale = (int?)null, encoding = (string?)null } },
            expected = new { rows = body.Length / 8, encoded_bytes = body.Length, file_sha256 = Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant() },
            max_row_bytes = 1024, file_identity = new { device = stat.Device, inode = stat.Inode, size = stat.Size, mtime_ns = stat.MtimeNs, ctime_ns = stat.CtimeNs } };
        return JobInputDescriptor.Parse(JsonSerializer.SerializeToUtf8Bytes(v));
    }
}
