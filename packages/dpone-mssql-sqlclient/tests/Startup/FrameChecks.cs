using System.Runtime.InteropServices;
using Dpone.SqlClient.Startup;

internal static class FrameChecks
{
    [DllImport("libc", SetLastError = true)] private static extern int pipe2(int[] descriptors, int flags);
    [DllImport("libc")] private static extern nint write(int fd, byte[] data, nuint size);
    [DllImport("libc")] private static extern int close(int fd);
    internal static void Run()
    {
        Verify(new byte[] { 0, 0, 0, 1, 42 }, true, null);
        Verify(new byte[] { 127, 255, 255, 255 }, true, "startup.frame_size");
        Verify(new byte[] { 0, 0, 0, 3, 42 }, true, "startup.truncated");
        Verify(new byte[] { 0, 0, 0, 1, 42, 43 }, true, "startup.trailing_frame");
        Verify(new byte[] { 0, 0, 0, 1, 42 }, false, "startup.deadline");
        int[] pair = Pipe();
        try
        {
            try { StartupFrame.Write(pair[1], new byte[1048576], StartupClock.NowNs() + 20000000, 1048576); throw new Exception("write_deadline_missing"); }
            catch (InvalidDataException e) { if (e.Message != "startup.deadline") throw; }
        }
        finally { close(pair[0]); close(pair[1]); }
        Console.WriteLine("PASS startup bounded framing: EOF/truncated/trailing/oversize/read-write deadlines");
    }
    private static int[] Pipe()
    {
        int[] pair = new int[2]; if (pipe2(pair, 0x800 | 0x80000) != 0) throw new Exception("pipe"); return pair;
    }
    private static void Verify(byte[] bytes, bool eof, string? expected)
    {
        int[] pair = Pipe();
        try
        {
            if (write(pair[1], bytes, (nuint)bytes.Length) != bytes.Length) throw new Exception("fixture_write");
            if (eof) { close(pair[1]); pair[1] = -1; }
            try
            {
                byte[] value = StartupFrame.Read(pair[0], StartupClock.NowNs() + 20000000);
                if (expected is not null || !value.SequenceEqual(new byte[] { 42 })) throw new Exception("frame_acceptance");
            }
            catch (InvalidDataException e) { if (e.Message != expected) throw; }
        }
        finally { close(pair[0]); if (pair[1] >= 0) close(pair[1]); }
    }
}
