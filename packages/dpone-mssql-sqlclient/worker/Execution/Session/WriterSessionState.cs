namespace Dpone.SqlClient.Execution.Session;

// Pure admission gate: no lock is held across provider calls. An overlapping or
// repeated attempt poisons the original operation as well as the new caller.
internal sealed class WriterSessionState
{
    private readonly object gate = new();
    private bool started, ready, busy, poisoned, disposed;
    internal bool IsPoisoned { get { lock (gate) return poisoned; } }
    internal void Begin(bool open)
    {
        lock (gate)
        {
            if (disposed || poisoned || busy || (open ? started : !ready)) Reject();
            if (open) started = true;
            busy = true;
        }
    }
    internal void Complete(bool open)
    {
        lock (gate)
        {
            if (disposed || poisoned || !busy) Reject();
            if (open) ready = true;
            busy = false;
        }
    }
    internal void RequireReady()
    {
        lock (gate) { if (disposed || poisoned || busy || !ready) Reject(); }
    }
    internal void Fail() { lock (gate) { poisoned = true; busy = false; } }
    internal bool BeginDispose()
    {
        lock (gate)
        {
            if (disposed) return false;
            if (busy) Reject();
            disposed = true; busy = true; return true;
        }
    }
    internal void EndDispose(bool failed)
    {
        lock (gate) { busy = false; poisoned |= failed; }
    }
    private void Reject() { poisoned = true; throw SessionFailure.Invalid(); }
}
