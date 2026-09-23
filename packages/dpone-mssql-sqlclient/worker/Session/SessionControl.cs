using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Session;

/// <summary>Original fenced owner supplied by trusted composition; parsing creates no authority.</summary>
public sealed record SessionOwnership
{
    /// <summary>Nonempty owner, at most256 Unicode scalar values, without ASCII controls.</summary>
    public string Owner { get; }
    /// <summary>Positive original fence.</summary>
    public long Fence { get; }
    /// <summary>Canonical nonzero supervisor incarnation.</summary>
    public string SupervisorId { get; }
    /// <summary>Validate immutable ownership scalars without observing external state.</summary>
    public SessionOwnership(string owner, long fence, string supervisorId)
    {
        SessionControlCodec.Owner(owner); SessionControlCodec.Positive(fence);
        SessionControlCodec.Uuid(supervisorId);
        (Owner, Fence, SupervisorId) = (owner, fence, supervisorId);
    }
}

/// <summary>Exact admitted stage incarnation; its name alone is never identity.</summary>
public sealed record SessionObjectIdentity
{
    /// <summary>Positive SQL object identifier.</summary>
    public int ObjectId { get; }
    /// <summary>Canonical object fingerprint.</summary>
    public string Fingerprint { get; }
    /// <summary>Validate immutable identity without SQL discovery.</summary>
    public SessionObjectIdentity(int objectId, string fingerprint)
    {
        SessionControlCodec.Positive(objectId); SessionControlCodec.Hash(fingerprint);
        (ObjectId, Fingerprint) = (objectId, fingerprint);
    }
}

/// <summary>Independent remote-session observation; never proof of writer exit or mutation authority.</summary>
public sealed record RemoteSessionIdentity
{
    /// <summary>Canonical nonzero physical connection UUID.</summary>
    public string ConnectionId { get; }
    /// <summary>SQL session identifier1..32767.</summary>
    public int SessionId { get; }
    /// <summary>Canonical naive server timestamp; never interpreted as UTC.</summary>
    public string ConnectTime { get; }
    /// <summary>Canonical naive server login timestamp.</summary>
    public string LoginTime { get; }
    /// <summary>Exact32-byte nonzero nonce in lowercase hexadecimal.</summary>
    public string Nonce { get; }
    /// <summary>Exact canonical authority digest, including an all-zero digest if supplied.</summary>
    public string AuthoritySha256 { get; }
    /// <summary>Validate canonical scalar spellings against the existing Python remote identity contract.</summary>
    public RemoteSessionIdentity(string connectionId, int sessionId, string connectTime, string loginTime, string nonce, string authoritySha256)
    {
        SessionControlCodec.Uuid(connectionId); SessionControlCodec.SessionId(sessionId);
        SessionControlCodec.Timestamp(connectTime); SessionControlCodec.Timestamp(loginTime);
        SessionControlCodec.Nonce(nonce); SessionControlCodec.Hash(authoritySha256);
        (ConnectionId, SessionId, ConnectTime, LoginTime, Nonce, AuthoritySha256) = (connectionId, sessionId, connectTime, loginTime, nonce, authoritySha256);
    }
}

/// <summary>Untrusted child locator requiring separate independent observation.</summary>
public sealed record SessionAnnouncement
{
    /// <summary>Original canonical launch digest.</summary>
    public string LaunchSha256 { get; }
    /// <summary>Original attempt binding.</summary>
    public string AttemptSha256 { get; }
    /// <summary>Reported SQL session identifier.</summary>
    public int SessionId { get; }
    /// <summary>Exact nonzero session nonce.</summary>
    public string Nonce { get; }
    /// <summary>Construct a bounded locator; this permits no bulk copy.</summary>
    public SessionAnnouncement(string launchSha256, string attemptSha256, int sessionId, string nonce)
    {
        SessionControlCodec.Hash(launchSha256); SessionControlCodec.Hash(attemptSha256);
        SessionControlCodec.SessionId(sessionId); SessionControlCodec.Nonce(nonce);
        (LaunchSha256, AttemptSha256, SessionId, Nonce) = (launchSha256, attemptSha256, sessionId, nonce);
    }
    /// <summary>Bind before independent observation using trusted launch and nonce expectations.</summary>
    public void Validate(StartupLaunch launch, ReadOnlySpan<byte> nonce, long nowNs)
    {
        SessionControlCodec.Current(launch, nowNs);
        if (nonce.Length != 32 || nonce.IndexOfAnyExcept((byte)0) < 0 || LaunchSha256 != launch.Digest ||
            AttemptSha256 != launch.AttemptSha256 || Nonce != Convert.ToHexString(nonce).ToLowerInvariant())
            throw SessionControlCodec.Invalid();
    }
}

/// <summary>Closed one-shot permission body. Structural admission neither sends a grant nor executes SQL.</summary>
public sealed class BulkGrant
{
    /// <summary>Canonical nonzero grant incarnation.</summary>
    public string GrantId { get; }
    /// <summary>Canonical launch binding.</summary>
    public string LaunchSha256 { get; }
    /// <summary>Immutable attempt binding.</summary>
    public string AttemptSha256 { get; }
    /// <summary>Original fence and owner.</summary>
    public SessionOwnership Ownership { get; }
    /// <summary>Admitted local process incarnation.</summary>
    public StartupProcessIdentity Process { get; }
    /// <summary>Exact owned stage incarnation.</summary>
    public SessionObjectIdentity ObjectIdentity { get; }
    /// <summary>Admitted native input binding.</summary>
    public string InputBindingSha256 { get; }
    /// <summary>Admitted build identity.</summary>
    public string BuildSha256 { get; }
    /// <summary>Independent remote observation, never settlement proof.</summary>
    public RemoteSessionIdentity RemoteSession { get; }
    /// <summary>Immutable observation artifact byte SHA256, distinct from semantic wire digest.</summary>
    public string WriterObservationSha256 { get; }
    /// <summary>Original nonrenewable operation deadline in CLOCK_MONOTONIC nanoseconds.</summary>
    public long OperationDeadlineNs { get; }
    /// <summary>Independently resolved principal; worker compares its own actual context before copy.</summary>
    public ResolvedDatabasePrincipal ResolvedPrincipal { get; }
    internal BulkGrant(string grantId, string launchSha256, string attemptSha256, SessionOwnership ownership,
        StartupProcessIdentity process, SessionObjectIdentity objectIdentity, string inputBindingSha256, string buildSha256,
        RemoteSessionIdentity remoteSession, string writerObservationSha256, long operationDeadlineNs, ResolvedDatabasePrincipal resolvedPrincipal)
        => (GrantId, LaunchSha256, AttemptSha256, Ownership, Process, ObjectIdentity, InputBindingSha256, BuildSha256,
            RemoteSession, WriterObservationSha256, OperationDeadlineNs, ResolvedPrincipal) = (grantId, launchSha256, attemptSha256, ownership,
            process, objectIdentity, inputBindingSha256, buildSha256, remoteSession, writerObservationSha256, operationDeadlineNs, resolvedPrincipal);
    /// <summary>
    /// Compare every original binding to independently supplied facts. Callers must
    /// separately reassert the fence and continuity of the same open connection;
    /// expectations copied from this grant provide no independent verification.
    /// </summary>
    public void Validate(StartupLaunch launch, SessionOwnership ownership, SessionObjectIdentity objectIdentity,
        RemoteSessionIdentity remoteSession, string writerObservationSha256, ResolvedDatabasePrincipal resolvedPrincipal, long nowNs)
    {
        SessionControlCodec.Current(launch, nowNs); SessionControlCodec.Hash(writerObservationSha256);
        if (ownership is null || objectIdentity is null || remoteSession is null || resolvedPrincipal is null || ResolvedPrincipal != resolvedPrincipal || LaunchSha256 != launch.Digest ||
            AttemptSha256 != launch.AttemptSha256 || Ownership != ownership || Process != launch.Process ||
            ObjectIdentity != objectIdentity || InputBindingSha256 != launch.InputBindingSha256 || BuildSha256 != launch.BuildSha256 ||
            RemoteSession != remoteSession || WriterObservationSha256 != writerObservationSha256 || OperationDeadlineNs != launch.OperationDeadlineNs)
            throw SessionControlCodec.Invalid();
    }
}

/// <summary>Catalog-resolved principal; comparing it with the worker token grants no additional capabilities.</summary>
public sealed record ResolvedDatabasePrincipal
{
    /// <summary>Exact target database principal identifier.</summary>
    public int PrincipalId { get; }
    /// <summary>Exact SQL identifier, bounded by UTF-16 code units.</summary>
    public string Name { get; }
    /// <summary>Canonical lowercase hexadecimal SID, one to85 bytes.</summary>
    public string Sid { get; }
    /// <summary>Validate the bounded identity independently of any SQL or grant.</summary>
    public ResolvedDatabasePrincipal(int principalId, string name, string sid)
    {
        SessionControlCodec.Owner(name);
        if (principalId < 1 || name.Length > 128 || sid is null || sid.Length is < 2 or > 170 ||
            sid.Length % 2 != 0 || sid.Any(c => !(c is >= '0' and <= '9' or >= 'a' and <= 'f')))
            throw SessionControlCodec.Invalid();
        (PrincipalId, Name, Sid) = (principalId, name, sid);
    }
}
