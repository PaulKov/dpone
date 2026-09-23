using System.Globalization;
using Dpone.SqlClient.Execution.Session;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Execution;

/// <summary>One-shot child comparison of an authenticated parent grant.</summary>
internal sealed class WorkerGrantAdmission
{
    private int attempted;

    internal BulkGrant Accept(byte[] body, StartupLaunch launch, SqlClientJob job,
        OwnSqlSession initial, OwnSqlSession current, long nowNs)
    {
        if (Interlocked.Exchange(ref attempted, 1) != 0) throw Invalid();
        try
        {
            WorkerJobAdmission.RequireBindings(job, launch, nowNs);
            if (job.Credentials is null || job.SessionNonce is null || initial is null || current != initial)
                throw Invalid();
            current.RequireCredentials(job.Credentials.Database, job.Credentials.Username);
            var grant = SessionControlCodec.DecodeGrant(body);
            if (grant.LaunchSha256 != launch.Digest || grant.AttemptSha256 != launch.AttemptSha256 ||
                grant.Process != launch.Process || grant.BuildSha256 != launch.BuildSha256 ||
                grant.InputBindingSha256 != launch.InputBindingSha256 || grant.Ownership != job.Ownership ||
                grant.ObjectIdentity != job.ObjectIdentity || grant.OperationDeadlineNs != launch.OperationDeadlineNs ||
                grant.RemoteSession.SessionId != current.SessionId || grant.RemoteSession.Nonce != current.Nonce ||
                current.Nonce != job.SessionNonce || grant.ResolvedPrincipal != current.Principal ||
                grant.RemoteSession.LoginTime != current.LoginTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture))
                throw Invalid();
            // Server connection_id/connect_time and authority/observation digests
            // are unchanged parent attestations. ClientConnectionId is NOT that
            // server identifier. The restricted child must not fabricate a full
            // independently observed RemoteSessionIdentity from this grant.
            return grant;
        }
        catch (Exception) { throw Invalid(); }
    }

    private static InvalidDataException Invalid() => new("worker.grant_invalid");
}
