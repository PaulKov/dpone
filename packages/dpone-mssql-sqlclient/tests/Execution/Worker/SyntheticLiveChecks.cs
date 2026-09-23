using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Execution;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;

// Explicit test-only post-startup exchange. Frozen originals are emitted before
// WorkerRun; only the separate root harness observes SQL or constructs a grant.
internal static class SyntheticLiveChecks
{
    internal static async Task<int> Run()
    {
        try
        {
            using var control = new SyntheticControl();
            // No worker deadline exists before the initial message. This finite
            // test-control admission allowance is never a worker renewal.
            using JsonDocument initial = await control.ReadLine(checked(StartupClock.NowNs() + 60_000_000_000));
            JsonElement value = initial.RootElement;
            JsonWire.Shape(value, "case_name", "job", "launch", "input_base64", "duration_ms");
            string label = JsonWire.Text(value, "case_name");
            if (label.Length is < 1 or > 128 || label.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not ('_' or '-' or '.')))
                throw SyntheticPipeFixture.Invalid();
            long duration = JsonWire.Integer(value, "duration_ms", 1000, 60000);
            long deadline = checked(StartupClock.NowNs() + duration * 1_000_000);
            using var fixture = SyntheticPipeFixture.Create(value, deadline);
            await control.WriteEvent(new JsonObject { ["event"] = "prepared",
                ["launch"] = JsonNode.Parse(fixture.LaunchBytes), ["job_binding"] = JsonNode.Parse(fixture.BindingBytes) }, deadline);
            using JsonDocument start = await control.ReadLine(deadline);
            JsonWire.Shape(start.RootElement, "action");
            if (JsonWire.Text(start.RootElement, "action") != "start") throw SyntheticPipeFixture.Invalid();
            return await Execute(control, fixture, deadline);
        }
        catch (Exception)
        {
            // Supplied Job and SDK exceptions must never reach either console.
            Console.Error.WriteLine("synthetic.protocol_invalid");
            return 2;
        }
    }
    private static async Task<int> Execute(SyntheticControl control, SyntheticPipeFixture fixture, long deadline)
    {
        using var cancel = new CancellationTokenSource();
        var worker = new WorkerRun();
        Task<int> running = Task.Run(() => worker.RunAsync(fixture.Context, true, cancel.Token));
        Task sending = Task.Run(fixture.SendJob);
        try
        {
            await sending;
            bool observed = false;
            while (!running.IsCompleted)
            {
                // Give an actual result priority over an announcement. A failed
                // open/decoder can close the session pipe without announcing.
                if (!fixture.ResultBroken) fixture.Result.Pump();
                fixture.Announcement.Pump();
                if (fixture.Result.Body is null && !fixture.Result.Broken && !fixture.Result.Eof &&
                    !observed && fixture.Announcement.Body is byte[] announcement)
                {
                    SessionAnnouncement parsed = SessionControlCodec.DecodeAnnouncement(announcement);
                    parsed.Validate(fixture.Launch, Convert.FromHexString(fixture.Job.SessionNonce!), StartupClock.NowNs());
                    observed = true;
                    await control.WriteEvent(new JsonObject { ["event"] = "observe", ["announcement"] = JsonNode.Parse(announcement) }, deadline);
                    using JsonDocument action = await control.ReadLine(deadline);
                    await ApplyAction(action.RootElement, fixture);
                }
                if (StartupClock.NowNs() >= deadline) { cancel.Cancel(); break; }
                if (fixture.Announcement.Broken) throw SyntheticPipeFixture.Invalid();
                if (!running.IsCompleted) await Task.Delay(2);
            }
            // No renewed timeout and no foreign close of worker-owned handles.
            // Root Popen containment must bound an unexpectedly blocked SDK/close.
            int exit = await running;
            if (!fixture.ResultBroken) fixture.Result.Pump();
            byte[]? delivered = fixture.ResultBroken ? null : fixture.Result.Body;
            byte[]? retained = worker.RetainedResult;
            bool matches = retained is not null && delivered is not null && retained.SequenceEqual(delivered);
            string status = fixture.ResultBroken || fixture.Result.Broken ? "broken" : delivered is null ? "missing" : "complete";
            // Compare exact raw bytes first. JSON projections do not substitute
            // for transport EOF or turn missing delivery into a passing result.
            control.WriteCompletion(new JsonObject { ["event"] = "complete", ["exit_code"] = exit,
                ["retained_result"] = retained is null ? null : JsonNode.Parse(retained),
                ["delivered_result"] = delivered is null ? null : JsonNode.Parse(delivered),
                ["delivery_status"] = status, ["retention_matches"] = matches });
            return exit; // Root requires process exit to match this worker report.
        }
        catch
        {
            cancel.Cancel();
            fixture.AbortParentWrites();
            // Await both owners before fixture disposal; do not race a foreign
            // FD close/reuse against active worker or credential writer calls.
            try { await sending; } catch { }
            try { await running; } catch { }
            throw;
        }
    }
    private static Task ApplyAction(JsonElement value, SyntheticPipeFixture fixture)
    {
        string action = JsonWire.Text(value, "action");
        if (action == "withhold")
        {
            JsonWire.Shape(value, "action");
            return Task.CompletedTask; // Keep grant writer open under original budget.
        }
        if (action != "grant") throw SyntheticPipeFixture.Invalid();
        JsonWire.Shape(value, "action", "grant", "break_result");
        JsonElement broken = value.GetProperty("break_result");
        if (broken.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) throw SyntheticPipeFixture.Invalid();
        byte[] grant = Encoding.UTF8.GetBytes(value.GetProperty("grant").GetRawText());
        _ = SessionControlCodec.DecodeGrant(grant); // Shape only; WorkerRun checks authority.
        fixture.SendGrant(grant, broken.GetBoolean());
        return Task.CompletedTask;
    }
}
