using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Session;

/// <summary>Use the original Linux wire epoch when injecting bulk operation deadlines.</summary>
public sealed class SessionClock : TimeProvider
{
    /// <summary>The original parent and startup timestamps are integer nanoseconds.</summary>
    public override long TimestampFrequency => 1_000_000_000;

    /// <summary>Read the existing CLOCK_MONOTONIC source, without an epoch conversion.</summary>
    public override long GetTimestamp() => StartupClock.NowNs();
}
