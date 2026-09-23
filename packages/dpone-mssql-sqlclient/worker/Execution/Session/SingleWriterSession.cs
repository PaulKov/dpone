using System.Data;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Session;
using Microsoft.Data.SqlClient;

namespace Dpone.SqlClient.Execution.Session;

/// <summary>
/// Exclusive owner of exactly one restricted SqlConnection. Opens once, sets the
/// admitted nonce only after empty-context observation, and verifies the accessible
/// original session before and after copying. Parent observation and grant remain
/// separate authority; this component never synthesizes them or stores outcomes.
/// </summary>
public sealed class SingleWriterSession : IDisposable
{
    private readonly SqlConnection connection;
    private readonly SqlRetryLogicBaseProvider noRetry;
    private readonly SqlClientCredentials credentials;
    private readonly BulkDeadline deadline;
    private readonly bool allowDisposableTest;
    private readonly string nonce;
    private readonly WriterSessionState state = new();
    private OwnSqlSession? original;
    /// <summary>Validate fixed policy without connecting; retain credentials only in memory.</summary>
    public SingleWriterSession(SqlClientCredentials credentials, string nonce, BulkDeadline deadline, bool allowDisposableTest)
    {
        try
        {
            WriterConnectionPolicy.Admit(credentials, allowDisposableTest);
            SessionControlCodec.Nonce(nonce);
            this.deadline = deadline ?? throw SessionFailure.Invalid();
            _ = deadline.Remaining();
            this.credentials = credentials; this.nonce = nonce; this.allowDisposableTest = allowDisposableTest;
            noRetry = SqlConfigurableRetryFactory.CreateNoneRetryProvider();
            connection = new SqlConnection { RetryLogicProvider = noRetry };
        }
        catch (Exception) { throw SessionFailure.Invalid(); }
    }
    /// <summary>Whether an ambiguous, repeated or overlapping action permanently revoked this session's use.</summary>
    public bool IsPoisoned => state.IsPoisoned;
    /// <summary>
    /// Borrow the same already-open connection only for the existing Bulk adapter,
    /// after independent grant admission. Caller must not close, reopen, reconfigure,
    /// impersonate, enlist, or use it concurrently. Return control before RequireSame.
    /// Neither this property nor OpenAsync grants SQL mutation permission.
    /// </summary>
    public SqlConnection Connection
    {
        get
        {
            state.Begin(false);
            try { RequireConnection(); state.Complete(false); return connection; }
            catch (Exception) { state.Fail(); throw SessionFailure.Failed(); }
        }
    }
    /// <summary>Open once under the original deadline; reject prior context, wrong login/database and unstable nonce setup.</summary>
    public async Task<OwnSqlSession> OpenAsync(CancellationToken cancellation = default)
    {
        state.Begin(true);
        try
        {
            cancellation.ThrowIfCancellationRequested(); _ = deadline.Remaining();
            connection.ConnectionString = WriterConnectionPolicy.Build(credentials, allowDisposableTest, deadline.SdkTimeoutSeconds()).ConnectionString;
            using var timer = deadline.CreateCancellation();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timer.Token);
            await connection.OpenAsync(linked.Token).ConfigureAwait(false);
            linked.Token.ThrowIfCancellationRequested(); _ = deadline.Remaining();
            RequireConnection();
            OwnSqlSession before = await WriterSessionSql.Observe(connection, deadline, linked.Token).ConfigureAwait(false);
            before.RequireCredentials(credentials.Database, credentials.Username);
            if (before.Nonce is not null) throw SessionFailure.Invalid();
            await WriterSessionSql.SetNonce(connection, nonce, deadline, linked.Token).ConfigureAwait(false);
            OwnSqlSession after = await WriterSessionSql.Observe(connection, deadline, linked.Token).ConfigureAwait(false);
            after.RequireCredentials(credentials.Database, credentials.Username);
            after.RequireSameIncarnation(before);
            if (after.Nonce != nonce) throw SessionFailure.Invalid();
            RequireConnection(); linked.Token.ThrowIfCancellationRequested(); _ = deadline.Remaining();
            original = after;
            state.Complete(true);
            return after;
        }
        catch (Exception) { state.Fail(); throw SessionFailure.Failed(); }
    }
    /// <summary>Bounded pre/post-copy continuity over the same connection and every accessible original field.</summary>
    public async Task<OwnSqlSession> RequireSameAsync(CancellationToken cancellation = default)
    {
        state.Begin(false);
        try
        {
            cancellation.ThrowIfCancellationRequested(); RequireConnection();
            OwnSqlSession observed = await WriterSessionSql.Observe(connection, deadline, cancellation).ConfigureAwait(false);
            if (original is null || observed != original) throw SessionFailure.Invalid();
            RequireConnection(); cancellation.ThrowIfCancellationRequested(); _ = deadline.Remaining();
            state.Complete(false);
            return observed;
        }
        catch (Exception) { state.Fail(); throw SessionFailure.Failed(); }
    }
    /// <summary>
    /// Synchronous continuity seam for the existing final Func&lt;InputReceipt&gt;
    /// callback: invoke this before requiring input completeness. The async SQL path
    /// retains its original timer; no timeout or authority is renewed by blocking here.
    /// </summary>
    public OwnSqlSession RequireSame(CancellationToken cancellation = default)
        => RequireSameAsync(cancellation).GetAwaiter().GetResult();
    private void RequireConnection()
    {
        _ = deadline.Remaining();
        if (connection.State != ConnectionState.Open || connection.ClientConnectionId == Guid.Empty ||
            (original is not null && connection.ClientConnectionId != original.ClientConnectionId)) throw SessionFailure.Invalid();
        var options = new SqlConnectionStringBuilder(connection.ConnectionString);
        if (options.Pooling || options.MultipleActiveResultSets || options.Enlist || options.ConnectRetryCount != 0 ||
            options.IntegratedSecurity || options.PersistSecurityInfo || options.Authentication != SqlAuthenticationMethod.SqlPassword ||
            !options.Encrypt.Equals(SqlConnectionEncryptOption.Mandatory) ||
            options.TrustServerCertificate != (credentials.TlsProfile == "disposable_test" && allowDisposableTest) ||
            !ReferenceEquals(connection.RetryLogicProvider, noRetry)) throw SessionFailure.Invalid();
    }
    /// <summary>
    /// Dispose synchronously once under exclusive ownership, never concurrently with
    /// a provider operation. No outcome object is held or changed here. Deadline
    /// exhaustion or provider failure is reported with a constant error. A blocked
    /// provider Dispose requires parent process kill/reap; no background task or new
    /// cleanup budget claims to preempt it. Cleanup is attempted even after expiry.
    /// </summary>
    public void Dispose()
    {
        if (!state.BeginDispose()) return;
        bool failed = false;
        try { _ = deadline.Remaining(); } catch (Exception) { failed = true; }
        try { connection.Dispose(); } catch (Exception) { failed = true; }
        try { _ = deadline.Remaining(); } catch (Exception) { failed = true; }
        state.EndDispose(failed);
        if (failed) throw SessionFailure.Failed();
    }
    /// <summary>Never format credentials, connection strings or snapshots as diagnostics.</summary>
    public override string ToString() => "SingleWriterSession [REDACTED]";
}
