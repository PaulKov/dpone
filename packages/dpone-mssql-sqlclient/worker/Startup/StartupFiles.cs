using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Dpone.SqlClient.Startup;

internal sealed record StartupFileIdentity(ulong Inode, ulong Size, uint DeviceMajor, uint DeviceMinor,
    ushort Mode, long ModifiedSeconds, uint ModifiedNanos, long ChangedSeconds, uint ChangedNanos);
internal static class StartupFiles
{
    [DllImport("libc", SetLastError = true)] private static extern int open(string path, int flags);
    [DllImport("libc", SetLastError = true)] private static extern int statx(int fd, string path, int flags, uint mask, byte[] data);
    [DllImport("libc", SetLastError = true)] private static extern nint fgetxattr(int fd, string name, IntPtr value, nuint size);
    internal static SafeFileHandle Open(string path)
    {
        int fd = open(path, 0x800 | 0x20000 | 0x80000); // RDONLY, NONBLOCK, NOFOLLOW, CLOEXEC.
        if (fd < 0) throw new InvalidDataException("startup.file_open");
        var handle = new SafeFileHandle((IntPtr)fd, ownsHandle: true);
        try
        {
            StartupFileIdentity identity = Identify(fd);
            if ((identity.Mode & 0xf000) != 0x8000 || (identity.Mode & 0xc00) != 0)
                throw new InvalidDataException("startup.file_type");
            nint capability = fgetxattr(fd, "security.capability", IntPtr.Zero, 0);
            if (capability > 0 || capability < 0 && Marshal.GetLastPInvokeError() is not (61 or 95 or 93))
                throw new InvalidDataException("startup.file_capability");
            return handle;
        }
        catch { handle.Dispose(); throw; }
    }
    internal static StartupFileIdentity Identify(int fd, string? path = null)
    {
        var bytes = new byte[256];
        if (statx(path is null ? fd : -100, path ?? "", path is null ? 0x1000 : 0x100, 0x7ff, bytes) != 0 || (BitConverter.ToUInt32(bytes, 0) & 0x3c3) != 0x3c3)
            throw new InvalidDataException("startup.file_identity");
        return new(BitConverter.ToUInt64(bytes, 32), BitConverter.ToUInt64(bytes, 40),
            BitConverter.ToUInt32(bytes, 136), BitConverter.ToUInt32(bytes, 140), BitConverter.ToUInt16(bytes, 28),
            BitConverter.ToInt64(bytes, 112), BitConverter.ToUInt32(bytes, 120), BitConverter.ToInt64(bytes, 96), BitConverter.ToUInt32(bytes, 104));
    }
}
