using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Execution;
using Dpone.SqlClient.Execution.Input;
using Dpone.SqlClient.Input;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;

// Post-startup component checks: real Linux pipes and descriptors, without
// claiming StartupBootstrap guards, SQL settlement or full parent integration.
internal static class WorkerPipeChecks
{
    internal static async Task Run(string fixtures)
    {
        foreach (string mode in new[] { "rows", "arrow" })
        {
            using var fixture = new PipeFixture(fixtures, mode);
            fixture.DeliverJob();
            var worker = new WorkerRun();
            Check(await worker.RunAsync(fixture.Context, false) == 0, "empty_exit");
            byte[] delivered = fixture.ReadResult();
            Check(delivered.SequenceEqual(worker.RetainedResult!), "exact_retention");
            SessionResult result = SessionResult.Decode(delivered, fixture.Launch, fixture.Expected, null);
            Check(result.Receipt == fixture.Expected && result.Error is null, "empty_receipt");
            fixture.RequireNoAnnouncement();
            fixture.RequireOriginalOpen();
            byte[] copy = worker.RetainedResult!; copy[0] ^= 1;
            Check(!copy.SequenceEqual(worker.RetainedResult!), "immutable_retention");
            try { await worker.RunAsync(fixture.Context, false); throw new Exception("test.run_reused"); }
            catch (InvalidOperationException) { }
        }
        using (var fixture = new PipeFixture(fixtures, "rows"))
        {
            fixture.DeliverJob();
            var worker = new WorkerRun();
            async Task<int> Compete()
            {
                try { return await worker.RunAsync(fixture.Context, false); }
                catch (InvalidOperationException) { return -1; }
            }
            int[] exits = await Task.WhenAll(Task.Run(Compete), Task.Run(Compete));
            Check(exits.Order().SequenceEqual(new[] { -1, 0 }), "concurrent_run_once");
            Check(fixture.ReadResult().SequenceEqual(worker.RetainedResult!), "concurrent_result_once");
            fixture.RequireNoAnnouncement(); fixture.RequireOriginalOpen();
        }
        using (var fixture = new PipeFixture(fixtures, "rows"))
        {
            fixture.DeliverJob(); fixture.BreakResult();
            var worker = new WorkerRun();
            Check(await worker.RunAsync(fixture.Context, false) == 1, "broken_pipe_exit");
            Check(SessionResult.Decode(worker.RetainedResult!, fixture.Launch, fixture.Expected, null).Receipt == fixture.Expected,
                "retain_before_failed_delivery");
            fixture.RequireNoAnnouncement(); fixture.RequireOriginalOpen();
        }
        using (var fixture = new PipeFixture(fixtures, "rows"))
        {
            fixture.DeliverMalformedJob();
            var worker = new WorkerRun();
            Check(await worker.RunAsync(fixture.Context, false) == 1, "malformed_exit");
            SessionResult result = SessionResult.Decode(fixture.ReadResult(), fixture.Launch, fixture.Expected, null);
            Check(result.Error == SessionResultError.Protocol && result.Receipt is null, "malformed_result");
            fixture.RequireNoAnnouncement(); fixture.RequireOriginalOpen();
        }
        using (var fixture = new PipeFixture(fixtures, "rows", 500_000_000))
        {
            fixture.DeliverJob(close: false);
            var worker = new WorkerRun();
            Check(await worker.RunAsync(fixture.Context, false) == 1, "withheld_eof_exit");
            Check(StartupClock.NowNs() >= fixture.Launch.OperationDeadlineNs, "original_deadline");
            SessionResult expired = SessionResult.Decode(worker.RetainedResult!, fixture.Launch, fixture.Expected, null);
            Check(expired.Receipt is null && expired.Error == SessionResultError.OperationTimeout, "deadline_classification");
            fixture.RequireNoAnnouncement(); fixture.RequireOriginalOpen();
        }
        Console.WriteLine("PASS actual Linux WorkerRun pipes: rows/Arrow empty, malformed Job, withheld EOF, broken result, retained bytes, one-shot and FD ownership");
    }
    private static void Check(bool value, string code) { if (!value) throw new Exception("test." + code); }

    private sealed class PipeFixture : IDisposable
    {
        [DllImport("libc", SetLastError = true)] private static extern int pipe2([Out] int[] pipe, int flags);
        [DllImport("libc", SetLastError = true)] private static extern int dup(int fd);
        [DllImport("libc", SetLastError = true)] private static extern nint read(int fd, byte[] value, nuint count);
        [DllImport("libc", SetLastError = true)] private static extern nint write(int fd, byte[] value, nuint count);
        private readonly HashSet<int> owned = new();
        private readonly FileStream original;
        private readonly string path;
        private readonly int credentials, announcement, result;
        private readonly byte[] job;
        internal StartupLaunch Launch { get; }
        internal StartupContext Context { get; }
        internal InputReceipt Expected { get; }
        internal PipeFixture(string fixtures, string mode, long duration = 20_000_000_000)
        {
            path = Path.Combine(Path.GetTempPath(), "dpone-worker-empty-" + Guid.NewGuid().ToString("N"));
            File.WriteAllBytes(path, Array.Empty<byte>());
            original = File.OpenRead(path);
            int input = dup((int)original.SafeFileHandle.DangerousGetHandle()); Check(input >= 3, "dup"); owned.Add(input);
            int[] startup = Pipe(), creds = Pipe(), session = Pipe(), grant = Pipe(), output = Pipe();
            credentials = creds[1]; announcement = session[0]; result = output[0];
            JobFileIdentity metadata = LinuxInputHandle.Observe(input);
            JsonNode node = JsonNode.Parse(File.ReadAllBytes(Path.Combine(fixtures, "empty.job.json")))!;
            node["input_mode"] = mode; node["input"]!["fd"] = input;
            node["input"]!["file_identity"] = new JsonObject { ["device"] = metadata.Device, ["inode"] = metadata.Inode,
                ["size"] = metadata.Size, ["mtime_ns"] = metadata.MtimeNs, ["ctime_ns"] = metadata.CtimeNs };
            SqlClientJob preliminary = SqlClientJob.Parse(Bytes(node));
            var expected = preliminary.Input.Native.Expected; Expected = new(expected.Rows, expected.Bytes, expected.Sha256);
            JsonNode launch = JsonNode.Parse(File.ReadAllBytes(Path.Combine(fixtures, "empty.launch.json")))!;
            launch["descriptors"] = new JsonObject { ["startup"] = startup[1], ["credentials"] = creds[0],
                ["session"] = session[1], ["grant"] = grant[0], ["result"] = output[1], ["input"] = input };
            launch["input_binding_sha256"] = preliminary.Input.Digest;
            long now = StartupClock.NowNs();
            launch["startup_deadline_ns"] = now + duration / 2; launch["operation_deadline_ns"] = now + duration;
            Launch = StartupLaunch.Parse(Bytes(launch)); node["launch_sha256"] = Launch.Digest;
            job = Bytes(node);
            Context = new StartupContext(Launch);
            foreach (int fd in new[] { creds[0], session[1], grant[0], output[1], input }) owned.Remove(fd);
            Close(startup[0]); Close(startup[1]);
            // Deliberately keep grant writer open with no data: empty execution
            // would expire if it tried to read grant or contact SQL.
        }
        private int[] Pipe()
        {
            var pair = new int[2]; Check(pipe2(pair, 2048) == 0, "pipe");
            foreach (int fd in pair) owned.Add(fd); return pair;
        }
        internal void DeliverJob(bool close = true)
        {
            StartupFrame.Write(credentials, job, Launch.OperationDeadlineNs, SqlClientJob.MaxJobBytes);
            if (close) Close(credentials);
        }
        internal void DeliverMalformedJob()
        {
            byte[] truncated = { 0, 0, 0, 5, 123 };
            Check(write(credentials, truncated, (nuint)truncated.Length) == truncated.Length, "test_write"); Close(credentials);
        }
        internal void BreakResult() => Close(result);
        internal byte[] ReadResult() => StartupFrame.Read(result, StartupClock.NowNs() + 2_000_000_000);
        internal void RequireNoAnnouncement() => Check(read(announcement, new byte[1], 1) == 0, "no_announcement_eof");
        internal void RequireOriginalOpen() => LinuxInputHandle.RequireStart((int)original.SafeFileHandle.DangerousGetHandle());
        private static byte[] Bytes(JsonNode node) => Encoding.UTF8.GetBytes(node.ToJsonString());
        private void Close(int fd) { if (owned.Remove(fd)) StartupFrame.Close(fd); }
        public void Dispose()
        {
            Context.Dispose();
            foreach (int fd in owned.ToArray()) Close(fd);
            original.Dispose(); File.Delete(path);
        }
    }
}
