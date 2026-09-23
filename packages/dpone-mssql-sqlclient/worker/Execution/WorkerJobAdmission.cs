using Dpone.SqlClient.Job;
using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Execution;

/// <summary>Bind an authenticated parent declaration; never invent parent authority.</summary>
internal static class WorkerJobAdmission
{
    internal static void RequireDeclaration(SqlClientJob job, StartupLaunch launch, bool allowDisposableTest, long nowNs)
    {
        RequireBindings(job, launch, nowNs);
        if (job.Credentials?.TlsProfile == "disposable_test" && !allowDisposableTest) throw Invalid();
    }

    // Parent separately validates original policy, ownership, stage and credential
    // intent before private delivery. These comparisons use the admitted launch.
    internal static void RequireBindings(SqlClientJob job, StartupLaunch launch, long nowNs)
    {
        if (job is null || launch is null || nowNs < 0 || nowNs >= launch.OperationDeadlineNs ||
            job.LaunchSha256 != launch.Digest || job.Identity.Digest != launch.AttemptSha256 ||
            job.Input.Digest != launch.InputBindingSha256 || job.Input.Fd != launch.Descriptors.Input)
            throw Invalid();
    }

    private static InvalidDataException Invalid() => new("worker.job_binding_invalid");
}
