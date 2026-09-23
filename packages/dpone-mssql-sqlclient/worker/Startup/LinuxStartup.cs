using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace Dpone.SqlClient.Startup;

/// <summary>Linux monotonic wire clock; never substitutes Stopwatch ticks.</summary>
public static class StartupClock
{
    [StructLayout(LayoutKind.Sequential)] private struct Timespec { public long Seconds, Nanoseconds; }
    [DllImport("libc", SetLastError = true)] private static extern int clock_gettime(int id, out Timespec value);
    /// <summary>Actual CLOCK_MONOTONIC integer nanoseconds.</summary>
    public static long NowNs()
    {
        if (clock_gettime(1, out Timespec value) != 0) throw new InvalidDataException("startup.clock");
        return checked(value.Seconds * 1000000000 + value.Nanoseconds);
    }
    /// <summary>Enforce an original absolute deadline without renewal.</summary>
    public static void RequireBefore(long deadlineNs)
    {
        if (deadlineNs <= 0 || NowNs() >= deadlineNs) throw new InvalidDataException("startup.deadline");
    }
}

internal static class LinuxStartup
{
    [StructLayout(LayoutKind.Sequential)] private struct Limit { public ulong Soft, Hard; }
    [DllImport("libc", SetLastError = true)] private static extern int prctl(int op, out int value, ulong a, ulong b, ulong c);
    [DllImport("libc", SetLastError = true)] private static extern int getrlimit(int resource, out Limit value);
    [DllImport("libc")] private static extern int getppid();
    [DllImport("libc", SetLastError = true)] private static extern int fcntl(int fd, int command);
    [DllImport("libc", SetLastError = true)] private static extern int statx(int fd, string path, int flags, uint mask, byte[] data);
    internal static StartupProcessIdentity Identity()
    {
        string machine = File.ReadAllText("/etc/machine-id").Trim();
        if (machine.Length != 32 || machine == new string('0', 32) || machine.Any(c => !(c is >= '0' and <= '9' or >= 'a' and <= 'f')))
            throw new InvalidDataException("startup.machine_identity");
        string? pidNamespace = new FileInfo("/proc/self/ns/pid").LinkTarget;
        if (pidNamespace is null || pidNamespace != new FileInfo("/proc/1/ns/pid").LinkTarget)
            throw new InvalidDataException("startup.namespace_identity");
        string host = Convert.ToHexString(SHA256.HashData(Encoding.ASCII.GetBytes(machine + "\n" + pidNamespace))).ToLowerInvariant();
        string boot = File.ReadAllText("/proc/sys/kernel/random/boot_id").Trim();
        if (!Guid.TryParseExact(boot, "D", out Guid parsed) || parsed.ToString("D") != boot)
            throw new InvalidDataException("startup.boot_identity");
        string raw = File.ReadAllText("/proc/self/stat");
        int opening = raw.IndexOf('('), closing = raw.LastIndexOf(')');
        if (opening < 1 || closing <= opening || int.Parse(raw[..opening].Trim()) != Environment.ProcessId)
            throw new InvalidDataException("startup.process_identity");
        string[] fields = raw[(closing + 1)..].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return new(host, boot, Environment.ProcessId, long.Parse(fields[19]));
    }
    internal static byte[] Verify(StartupLaunch launch)
    {
        if (!OperatingSystem.IsLinux() || RuntimeInformation.ProcessArchitecture != Architecture.Arm64)
            throw new InvalidDataException("startup.platform");
        if (getppid() != launch.ParentPid || Identity() != launch.Process)
            throw new InvalidDataException("startup.process_identity");
        if (prctl(2, out int death, 0, 0, 0) != 0 || death != 9)
            throw new InvalidDataException("startup.pdeathsig");
        if (getrlimit(9, out Limit space) != 0 || space.Soft != (ulong)launch.AddressSpaceBytes || space.Hard != space.Soft)
            throw new InvalidDataException("startup.address_space");
        if (getrlimit(4, out Limit core) != 0 || core.Soft != 0 || core.Hard != 0)
            throw new InvalidDataException("startup.core");
        if (Environment.Version.ToString() != "8.0.31" || System.Runtime.GCSettings.IsServerGC ||
            GC.GetGCMemoryInfo().TotalAvailableMemoryBytes != 536870912)
            throw new InvalidDataException("startup.runtime_profile");
        int[] all = launch.Descriptors.All;
        for (int i = 0; i < all.Length; i++)
        {
            int flags = fcntl(all[i], 3);
            byte[] metadata = new byte[256];
            if (flags < 0 || statx(all[i], "", 0x1000, 3, metadata) != 0)
                throw new InvalidDataException("startup.descriptor");
            int kind = BitConverter.ToUInt16(metadata, 28) & 0xf000;
            int mode = i is 0 or 2 or 4 ? 1 : 0;
            if ((flags & 3) != mode || kind != (i == 5 ? 0x8000 : 0x1000) || (i != 5 && (flags & 0x800) == 0))
                throw new InvalidDataException("startup.descriptor");
        }
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new {
            schema_version = 1, launch_sha256 = launch.Digest,
            process = new { host_sha256 = launch.Process.HostSha256, boot_id = launch.Process.BootId,
                pid = Environment.ProcessId, start_ticks = Identity().StartTicks },
            address_space_bytes = space.Soft, parent_death_signal = death, core_limit_bytes = core.Soft,
            runtime_version = Environment.Version.ToString(), gc_heap_limit_bytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
            server_gc = System.Runtime.GCSettings.IsServerGC, roll_forward = Environment.GetEnvironmentVariable("DOTNET_ROLL_FORWARD")
        });
    }
}
