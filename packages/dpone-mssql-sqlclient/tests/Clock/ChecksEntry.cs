using Dpone.SqlClient.Startup;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Bulk;

var clock = new SessionClock();
long before = StartupClock.NowNs();
long observed = clock.GetTimestamp();
long after = StartupClock.NowNs();
Check(before <= observed && observed <= after, "same_linux_epoch");
Check(clock.TimestampFrequency == 1_000_000_000, "nanosecond_frequency");
Check(clock.GetElapsedTime(observed, checked(observed + 1_000_000_000)) == TimeSpan.FromSeconds(1), "exact_elapsed_mapping");
var deadline = new BulkDeadline(clock, checked(observed + 10_000_000_000));
Check(deadline.Remaining() > TimeSpan.Zero && deadline.Remaining() <= TimeSpan.FromSeconds(10), "original_absolute_deadline");
using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(20), clock);
Check(cancellation.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(5)), "timer_cancellation");
var expired = new BulkDeadline(clock, before);
for (int i = 0; i < 2; i++)
{
    try { expired.Remaining(); throw new Exception("expired_renewed"); }
    catch (TimeoutException) { }
}
Console.WriteLine("PASS actual Linux clock epoch/frequency, bulk absolute deadline, timer cancellation and no expiry renewal");
static void Check(bool value, string code) { if (!value) throw new Exception(code); }
