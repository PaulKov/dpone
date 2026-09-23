using Dpone.SqlClient.Execution.Session;
using static SessionTestData;

internal static class SessionStateChecks
{
    internal static void Run()
    {
        var gate = new WriterSessionState(); gate.Begin(true); gate.Complete(true); gate.RequireReady();
        gate.Begin(false); gate.Complete(false); gate.RequireReady();
        Reject(() => gate.Begin(true)); Check(gate.IsPoisoned); Reject(gate.RequireReady);
        Check(gate.BeginDispose()); gate.EndDispose(false); Check(!gate.BeginDispose());
        gate = new WriterSessionState(); gate.Begin(true);
        Task.Run(() => Reject(() => gate.Begin(false))).GetAwaiter().GetResult();
        Check(gate.IsPoisoned); Reject(() => gate.BeginDispose()); Reject(() => gate.Complete(true));
        gate.Fail(); Check(gate.BeginDispose()); gate.EndDispose(false);
        gate = new WriterSessionState(); Reject(() => gate.Begin(false)); Check(gate.IsPoisoned);
        gate = new WriterSessionState(); gate.Begin(true); gate.Complete(true); gate.Begin(false);
        Reject(() => gate.Begin(false)); Check(gate.IsPoisoned); gate.Fail();
        using (var session = new SingleWriterSession(Credentials(), Nonce, Deadline(), false))
        {
            Check(session.ToString() == "SingleWriterSession [REDACTED]");
            using var cancel = new CancellationTokenSource(); cancel.Cancel();
            Reject(() => session.OpenAsync(cancel.Token).GetAwaiter().GetResult());
            Check(session.IsPoisoned); Reject(() => session.OpenAsync().GetAwaiter().GetResult());
            Reject(() => { _ = session.Connection; }); Reject(() => session.RequireSame());
        }
        using (var session = new SingleWriterSession(Credentials(), Nonce, Deadline(), false))
        { Reject(() => session.RequireSame()); Check(session.IsPoisoned); }
        Reject(() => new SingleWriterSession(Credentials("disposable_test"), Nonce, Deadline(), false));
        Reject(() => new SingleWriterSession(Credentials(), new string('0', 64), Deadline(), false));
        var clock = new TestClock();
        var expired = new SingleWriterSession(Credentials(), Nonce, Deadline(clock), false);
        clock.Now = 10_000_000_000;
        Reject(() => expired.OpenAsync().GetAwaiter().GetResult()); Check(expired.IsPoisoned);
        Reject(expired.Dispose); expired.Dispose();
        Console.WriteLine("PASS open-once, overlap/reentrancy poison, closed cleanup and no-I/O cancellation/deadline controls: " + Checks);
    }
}
