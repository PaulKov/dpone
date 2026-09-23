if (args.Length == 1 && args[0] == "synthetic-live")
{
    Environment.ExitCode = await SyntheticLiveChecks.Run();
    return;
}
if (args.Length != 0) throw new InvalidOperationException("test.mode_invalid");
SnapshotChecks.Run();
SessionPolicyChecks.Run();
SessionStateChecks.Run();
Console.WriteLine("PASS writer-session bounded managed checks; no live SQL performed");
