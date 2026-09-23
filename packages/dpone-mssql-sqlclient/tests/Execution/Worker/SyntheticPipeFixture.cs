using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Execution.Input;
using Dpone.SqlClient.Input;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Startup;

// Synthetic test parent only. Neither these declarations nor local process exit
// substitute for production startup admission, durable intent or SQL settlement.
internal sealed class SyntheticPipeFixture : IDisposable
{
    [DllImport("libc", SetLastError = true)] private static extern int pipe2([Out] int[] pipe, int flags);
    [DllImport("libc", SetLastError = true)] private static extern int dup(int fd);
    private readonly HashSet<int> owned = new();
    private FileStream? original;
    private string? path;
    private bool ownsFile;
    private StartupContext? context;
    private int credentials, grant;
    private bool jobSent, grantSent;
    internal StartupLaunch Launch { get; private set; } = null!;
    internal SqlClientJob Job { get; private set; } = null!;
    internal StartupContext Context => context ?? throw Invalid();
    internal SyntheticFrame Announcement { get; private set; } = null!;
    internal SyntheticFrame Result { get; private set; } = null!;
    internal bool ResultBroken { get; private set; }
    internal byte[] LaunchBytes { get; private set; } = null!;
    internal byte[] BindingBytes { get; private set; } = null!;
    internal InputReceipt Expected { get; private set; } = null!;
    internal static InvalidDataException Invalid() => new("synthetic.protocol_invalid");

    internal static SyntheticPipeFixture Create(JsonElement input, long deadline)
    {
        var fixture = new SyntheticPipeFixture();
        try { fixture.Initialize(input, deadline); return fixture; }
        catch { fixture.Dispose(); throw; }
    }
    private void Initialize(JsonElement input, long deadline)
    {
        if (!OperatingSystem.IsLinux()) throw Invalid();
        LinuxInputHandle.Platform(); StartupClock.RequireBefore(deadline);
        SqlClientJob supplied = SqlClientJob.Parse(Encoding.UTF8.GetBytes(input.GetProperty("job").GetRawText()));
        if (supplied.Input.Native.Expected.Rows == 0 || supplied.Credentials?.TlsProfile != "disposable_test") throw Invalid();
        byte[] bytes = Convert.FromBase64String(input.GetProperty("input_base64").GetString() ?? throw Invalid());
        var expected = supplied.Input.Native.Expected;
        if (bytes.Length != expected.Bytes || Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant() != expected.Sha256) throw Invalid();
        // Actual canonical decoder checks row count, scalar framing and EOF. No
        // native re-encoding, corrected expectation or second authority is made.
        using (var memory = new MemoryStream(bytes, writable: false))
        using (var reader = new NativeInputReader(memory, supplied.Input.Native, default, new InputBufferBudget(supplied.MaxInputBatchBytes)))
        {
            while (reader.Read()) StartupClock.RequireBefore(deadline);
            Expected = reader.RequireComplete();
        }
        path = Path.Combine(Path.GetTempPath(), "dpone-synthetic-worker-" + Guid.NewGuid().ToString("N"));
        using (var output = new FileStream(path, new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write,
            Share = FileShare.None, UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite }))
        { ownsFile = true; output.Write(bytes); }
        original = File.OpenRead(path);
        int inputFd = dup(checked((int)original.SafeFileHandle.DangerousGetHandle()));
        if (inputFd < 3) throw Invalid(); owned.Add(inputFd);
        int[] startup = Pipe(), creds = Pipe(), session = Pipe(), grantPipe = Pipe(), result = Pipe();
        credentials = creds[1]; grant = grantPipe[1];
        Announcement = new(session[0]); Result = new(result[0]);
        JobFileIdentity stat = LinuxInputHandle.Observe(inputFd); LinuxInputHandle.RequireStart(inputFd);
        JsonNode job = JsonNode.Parse(input.GetProperty("job").GetRawText())!;
        job["input"]!["fd"] = inputFd;
        job["input"]!["file_identity"] = new JsonObject { ["device"] = stat.Device, ["inode"] = stat.Inode,
            ["size"] = stat.Size, ["mtime_ns"] = stat.MtimeNs, ["ctime_ns"] = stat.CtimeNs };
        SqlClientJob preliminary = SqlClientJob.Parse(Bytes(job));
        // Validate the supplied closed launch before changing only wrapper-owned bindings.
        _ = StartupLaunch.Parse(Encoding.UTF8.GetBytes(input.GetProperty("launch").GetRawText()));
        JsonNode launch = JsonNode.Parse(input.GetProperty("launch").GetRawText())!;
        launch["descriptors"] = new JsonObject { ["startup"] = startup[1], ["credentials"] = creds[0],
            ["session"] = session[1], ["grant"] = grantPipe[0], ["result"] = result[1], ["input"] = inputFd };
        launch["startup_deadline_ns"] = deadline; launch["operation_deadline_ns"] = deadline;
        launch["input_binding_sha256"] = preliminary.Input.Digest;
        launch["attempt_sha256"] = preliminary.Identity.Digest;
        LaunchBytes = Bytes(launch); Launch = StartupLaunch.Parse(LaunchBytes);
        job["launch_sha256"] = Launch.Digest; Job = SqlClientJob.Parse(Bytes(job));
        BindingBytes = Job.BindingBytes();
        context = new StartupContext(Launch);
        foreach (int fd in new[] { creds[0], session[1], grantPipe[0], result[1], inputFd }) owned.Remove(fd);
        Close(startup[0]); Close(startup[1]); StartupClock.RequireBefore(deadline);
    }
    private int[] Pipe()
    {
        var pair = new int[2];
        if (pipe2(pair, 2048 | 0x80000) != 0) throw Invalid(); // NONBLOCK and CLOEXEC.
        foreach (int fd in pair) owned.Add(fd);
        return pair;
    }
    internal void SendJob()
    {
        if (jobSent) throw Invalid(); jobSent = true;
        // Secret encoding is solely for the private worker credential pipe.
        StartupFrame.Write(credentials, Job.Encode(), Launch.OperationDeadlineNs, SqlClientJob.MaxJobBytes);
        Close(credentials);
    }
    internal void SendGrant(byte[] body, bool breakResult)
    {
        if (grantSent) throw Invalid(); grantSent = true;
        if (breakResult) { Close(Result.Fd); ResultBroken = true; }
        StartupFrame.Write(grant, body, Launch.OperationDeadlineNs); Close(grant);
    }
    internal void AbortParentWrites()
    {
        try { Close(credentials); } finally { Close(grant); }
    }
    private void Close(int fd) { if (owned.Remove(fd)) StartupFrame.Close(fd); }
    internal static byte[] Bytes(JsonNode node) => Encoding.UTF8.GetBytes(node.ToJsonString());
    public void Dispose()
    {
        bool failed = false;
        try { context?.Dispose(); } catch { failed = true; }
        foreach (int fd in owned.ToArray()) try { Close(fd); } catch { failed = true; }
        try { original?.Dispose(); } catch { failed = true; }
        if (ownsFile && path is not null) try { File.Delete(path); } catch { failed = true; }
        if (failed) throw Invalid();
    }
}

// Incremental finite frame + EOF reader. Pump never waits, including after the
// original deadline: it can inspect bytes already delivered without renewal.
internal sealed class SyntheticFrame(int fd)
{
    private readonly byte[] data = new byte[16_389];
    private readonly byte[] scratch = new byte[4096];
    private int used;
    internal int Fd { get; } = fd;
    internal bool Eof { get; private set; }
    internal bool Broken { get; private set; }
    internal byte[]? Body
    {
        get
        {
            if (!Eof || Broken || used < 5) return null;
            uint size = BinaryPrimitives.ReadUInt32BigEndian(data);
            return size is > 0 and <= 16384 && used == size + 4 ? data.AsSpan(4, (int)size).ToArray() : null;
        }
    }
    internal void Pump()
    {
        while (!Eof && !Broken)
        {
            int n = SyntheticControl.ReadAvailable(Fd, scratch);
            if (n == -1) return;
            if (n == 0) { Eof = true; Broken = used != 0 && Body is null; return; }
            if (used + n > data.Length) { Broken = true; return; }
            scratch.AsSpan(0, n).CopyTo(data.AsSpan(used)); used += n;
            if (used >= 4)
            {
                uint size = BinaryPrimitives.ReadUInt32BigEndian(data);
                if (size is 0 or > 16384 || used > size + 4) { Broken = true; return; }
            }
        }
    }
}

// Only this control surface uses stdin/stdout. Worker credentials/results use
// distinct pipes; errors emitted by the caller never include caught messages.
internal sealed class SyntheticControl : IDisposable
{
    [DllImport("libc", SetLastError = true)] private static extern int fcntl(int fd, int operation, int value);
    [DllImport("libc", SetLastError = true)] private static extern nint read(int fd, byte[] bytes, nuint length);
    [DllImport("libc", SetLastError = true)] private static extern nint write(int fd, byte[] bytes, nuint length);
    private readonly int inputFlags, outputFlags;
    private readonly Queue<byte> pending = new();
    internal const int MaxLine = 1 << 20;
    internal SyntheticControl()
    {
        inputFlags = fcntl(0, 3, 0); outputFlags = fcntl(1, 3, 0);
        if (inputFlags < 0 || outputFlags < 0) throw SyntheticPipeFixture.Invalid();
        if (fcntl(0, 4, inputFlags | 2048) < 0) throw SyntheticPipeFixture.Invalid();
        if (fcntl(1, 4, outputFlags | 2048) < 0)
        { _ = fcntl(0, 4, inputFlags); throw SyntheticPipeFixture.Invalid(); }
    }
    internal static int ReadAvailable(int fd, byte[] buffer)
    {
        nint n = read(fd, buffer, (nuint)buffer.Length);
        if (n < 0 && Marshal.GetLastPInvokeError() is 4 or 11) return -1;
        if (n < 0 || n > buffer.Length) throw SyntheticPipeFixture.Invalid();
        return (int)n;
    }
    internal async Task<JsonDocument> ReadLine(long deadline)
    {
        using var bytes = new MemoryStream(); var buffer = new byte[4096];
        while (true)
        {
            StartupClock.RequireBefore(deadline);
            while (pending.Count != 0)
            {
                byte b = pending.Dequeue();
                if (b == 10)
                {
                    if (bytes.Length == 0) throw SyntheticPipeFixture.Invalid();
                    StartupClock.RequireBefore(deadline);
                    return JsonWire.Document(bytes.ToArray());
                }
                if (bytes.Length == MaxLine) throw SyntheticPipeFixture.Invalid();
                bytes.WriteByte(b);
            }
            int n = ReadAvailable(0, buffer);
            if (n == 0) throw SyntheticPipeFixture.Invalid();
            if (n == -1) { await Task.Delay(2); continue; }
            foreach (byte b in buffer.AsSpan(0, n).ToArray()) pending.Enqueue(b);
        }
    }
    internal async Task WriteEvent(JsonNode value, long deadline)
    {
        byte[] body = Encoding.UTF8.GetBytes(value.ToJsonString() + "\n");
        if (body.Length > MaxLine + 1) throw SyntheticPipeFixture.Invalid();
        int offset = 0;
        while (offset < body.Length)
        {
            StartupClock.RequireBefore(deadline);
            byte[] piece = body.AsSpan(offset, Math.Min(4096, body.Length - offset)).ToArray();
            nint n = write(1, piece, (nuint)piece.Length);
            if (n < 0 && Marshal.GetLastPInvokeError() is 4 or 11) { await Task.Delay(2); continue; }
            if (n <= 0 || n > piece.Length) throw SyntheticPipeFixture.Invalid();
            offset += (int)n;
        }
    }
    internal void WriteCompletion(JsonNode value)
    {
        // Test-only report of retained memory after expiry: one immediate write,
        // no poll, delay, retry or renewed operation/result delivery authority.
        byte[] body = Encoding.UTF8.GetBytes(value.ToJsonString() + "\n");
        if (body.Length > MaxLine + 1 || write(1, body, (nuint)body.Length) != body.Length)
            throw SyntheticPipeFixture.Invalid();
    }
    public void Dispose()
    {
        bool input = fcntl(0, 4, inputFlags) >= 0, output = fcntl(1, 4, outputFlags) >= 0;
        if (!input || !output) throw SyntheticPipeFixture.Invalid();
    }
}
