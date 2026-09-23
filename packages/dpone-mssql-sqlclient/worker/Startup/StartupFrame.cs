using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Dpone.SqlClient.Startup;

/// <summary>Bounded Linux nonblocking frame transport; ownership stays with the caller.</summary>
public static class StartupFrame
{
    [StructLayout(LayoutKind.Sequential)] private struct PollFd { public int Fd; public short Events, Returned; }
    [DllImport("libc", SetLastError = true)] private static extern int poll(ref PollFd fd, nuint count, int timeout);
    [DllImport("libc", SetLastError = true)] private static extern nint read(int fd, byte[] bytes, nuint length);
    [DllImport("libc", SetLastError = true)] private static extern nint write(int fd, byte[] bytes, nuint length);
    [DllImport("libc", SetLastError = true)] private static extern int close(int fd);
    private static void Wait(int fd, bool writing, long deadline)
    {
        while (true)
        {
            StartupClock.RequireBefore(deadline);
            var request = new PollFd { Fd = fd, Events = (short)(writing ? 4 : 1) };
            int milliseconds = (int)Math.Min(50, Math.Max(1, (deadline - StartupClock.NowNs()) / 1000000));
            int count = poll(ref request, 1, milliseconds);
            StartupClock.RequireBefore(deadline);
            if (count < 0 && Marshal.GetLastPInvokeError() == 4) continue;
            if (count < 0 || (request.Returned & (8 | 32)) != 0 || writing && (request.Returned & 16) != 0)
                throw new InvalidDataException("startup.channel");
            if (count > 0 && (request.Returned & (writing ? 4 : 1 | 16)) != 0) return;
        }
    }
    private static int ReadSome(int fd, byte[] bytes, int count, long deadline)
    {
        while (true)
        {
            Wait(fd, false, deadline);
            int n = (int)read(fd, bytes, (nuint)count);
            if (n < 0 && Marshal.GetLastPInvokeError() is 4 or 11) continue;
            if (n < 0 || n > count) throw new InvalidDataException("startup.channel");
            StartupClock.RequireBefore(deadline);
            return n;
        }
    }
    private static byte[] Exact(int fd, int size, long deadline)
    {
        var result = new byte[size];
        var scratch = new byte[Math.Min(4096, size)];
        int offset = 0;
        while (offset < size)
        {
            int count = ReadSome(fd, scratch, Math.Min(scratch.Length, size - offset), deadline);
            if (count == 0) throw new InvalidDataException("startup.truncated");
            Buffer.BlockCopy(scratch, 0, result, offset, count); offset += count;
        }
        return result;
    }
    /// <summary>Read one finite body and actual EOF; reject trailing bytes and withheld EOF.</summary>
    public static byte[] Read(int fd, long deadlineNs, int maxPayload = 16384)
    {
        if (maxPayload is < 1 or > 1048576) throw new InvalidDataException("startup.frame_limit");
        uint size = BinaryPrimitives.ReadUInt32BigEndian(Exact(fd, 4, deadlineNs));
        if (size == 0 || size > maxPayload) throw new InvalidDataException("startup.frame_size");
        byte[] body = Exact(fd, (int)size, deadlineNs);
        if (ReadSome(fd, new byte[1], 1, deadlineNs) != 0) throw new InvalidDataException("startup.trailing_frame");
        return body;
    }
    /// <summary>Write one bounded frame under the original deadline; no retries after caller failure.</summary>
    public static void Write(int fd, byte[] body, long deadlineNs, int maxPayload = 16384)
    {
        if (maxPayload is < 1 or > 1048576 || body.Length < 1 || body.Length > maxPayload)
            throw new InvalidDataException("startup.frame_size");
        var frame = new byte[checked(body.Length + 4)];
        BinaryPrimitives.WriteInt32BigEndian(frame, body.Length); Buffer.BlockCopy(body, 0, frame, 4, body.Length);
        int offset = 0;
        while (offset < frame.Length)
        {
            Wait(fd, true, deadlineNs);
            byte[] chunk = frame.AsSpan(offset, Math.Min(4096, frame.Length - offset)).ToArray();
            int count = (int)write(fd, chunk, (nuint)chunk.Length);
            if (count < 0 && Marshal.GetLastPInvokeError() is 4 or 11) continue;
            if (count <= 0 || count > chunk.Length) throw new InvalidDataException("startup.channel");
            offset += count;
        }
        StartupClock.RequireBefore(deadlineNs);
    }
    internal static void Close(int fd)
    {
        if (close(fd) != 0) throw new InvalidDataException("startup.close");
    }
}
