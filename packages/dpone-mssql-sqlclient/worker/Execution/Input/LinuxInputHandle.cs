using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Dpone.SqlClient.Job;
using Microsoft.Win32.SafeHandles;

namespace Dpone.SqlClient.Execution.Input;

/// <summary>Narrow Linux Arm64 descriptor observations, using the fixed Linux UAPI statx layout.</summary>
internal static class LinuxInputHandle
{
    [DllImport("libc", SetLastError = true)] private static extern int fcntl(int fd, int command);
    [DllImport("libc", SetLastError = true)] private static extern long lseek(int fd, long offset, int origin);
    [DllImport("libc", SetLastError = true)] private static extern int statx(int fd, string path, int flags, uint mask, [Out] byte[] data);
    [DllImport("libc", SetLastError = true)] private static extern nint read(SafeFileHandle fd, IntPtr buffer, nuint count);
    internal static InvalidDataException Invalid() => new("input.sealed_descriptor_invalid");
    internal static void Platform()
    {
        if (!OperatingSystem.IsLinux() || RuntimeInformation.ProcessArchitecture != Architecture.Arm64 || !BitConverter.IsLittleEndian)
            throw Invalid();
    }
    internal static void RequireStart(int fd)
    {
        if (lseek(fd, 0, 1) != 0) throw Invalid();
    }
    internal static JobFileIdentity Observe(int fd)
    {
        int flags = fcntl(fd, 3); // F_GETFL: O_RDONLY == 0; O_PATH is not readable.
        if (flags < 0 || (flags & 3) != 0 || (flags & 0x200000) != 0) throw Invalid();
        var data = new byte[256];
        // AT_EMPTY_PATH queries the exact fd, without opening or following a pathname.
        // STATX_BASIC_STATS requests the stable UAPI fields. Require TYPE, INO,
        // SIZE, MTIME and CTIME; device major/minor are unconditional statx fields.
        const uint required = 0x1 | 0x100 | 0x200 | 0x40 | 0x80;
        if (statx(fd, "", 0x1000, 0x7ff, data) != 0 || (U32(data, 0) & required) != required ||
            (BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(28)) & 0xf000) != 0x8000) throw Invalid();
        // Linux include/uapi/linux/stat.h: ino32, size40, ctime96, mtime112,
        // dev_major136, dev_minor140; each timestamp is s64 seconds/u32 nanos/reserved.
        ulong major = U32(data, 136), minor = U32(data, 140);
        ulong device = ((major & 0xfff) << 8) | (minor & 0xff) | ((major & ~0xfffUL) << 32) | ((minor & ~0xffUL) << 12);
        ulong size = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(40));
        if (size > long.MaxValue) throw Invalid();
        return new(device, BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(32)), (long)size, Timestamp(data, 112), Timestamp(data, 96));
    }
    private static uint U32(byte[] data, int offset) => BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset));
    private static long Timestamp(byte[] data, int offset)
    {
        long seconds = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(offset));
        uint nanos = U32(data, offset + 8);
        if (seconds < 0 || nanos >= 1_000_000_000) throw Invalid();
        try { return checked(seconds * 1_000_000_000 + nanos); }
        catch (OverflowException) { throw Invalid(); }
    }
    internal static int Read(SafeFileHandle handle, byte[] buffer, int offset, int count)
    {
        GCHandle pinned = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        try
        {
            nint result = read(handle, IntPtr.Add(pinned.AddrOfPinnedObject(), offset), (nuint)count);
            if (result < 0 || result > count) throw Invalid(); // Including EINTR: never retry/renew.
            return (int)result;
        }
        finally { pinned.Free(); }
    }
}
