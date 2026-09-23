using System.Text;
using Dpone.SqlClient.Session;

namespace Dpone.SqlClient.Execution.Session;

/// <summary>
/// Immutable current-session observation accessible to a restricted SQL login.
/// ClientConnectionId is a client continuity diagnostic, never parent-observed
/// connection/connect_time authority. No remote-session or authority digest is invented.
/// </summary>
public sealed record OwnSqlSession
{
    /// <summary>Current SQL session identifier.</summary>
    public int SessionId { get; }
    /// <summary>Exact context nonce; null only for the internal pre-SET empty observation.</summary>
    public string? Nonce { get; }
    /// <summary>Current database identifier.</summary>
    public int DatabaseId { get; }
    /// <summary>Exact current database name.</summary>
    public string DatabaseName { get; }
    /// <summary>Current server login name.</summary>
    public string LoginName { get; }
    /// <summary>Canonical current server login SID.</summary>
    public string LoginSid { get; }
    /// <summary>Original server login name.</summary>
    public string OriginalLoginName { get; }
    /// <summary>Canonical original server login SID.</summary>
    public string OriginalLoginSid { get; }
    /// <summary>Catalog-resolved current database principal; its SID may differ from the server login SID.</summary>
    public ResolvedDatabasePrincipal Principal { get; }
    /// <summary>Naive server login timestamp, without timezone conversion or connection-time substitution.</summary>
    public DateTime LoginTime { get; }
    /// <summary>SqlConnection client diagnostic retained separately from server observations.</summary>
    public Guid ClientConnectionId { get; }
    private OwnSqlSession(int sessionId, string? nonce, int databaseId, string databaseName, string loginName,
        string loginSid, string originalLoginName, string originalLoginSid, ResolvedDatabasePrincipal principal,
        DateTime loginTime, Guid clientConnectionId)
        => (SessionId, Nonce, DatabaseId, DatabaseName, LoginName, LoginSid, OriginalLoginName, OriginalLoginSid,
            Principal, LoginTime, ClientConnectionId) = (sessionId, nonce, databaseId, databaseName, loginName,
            loginSid, originalLoginName, originalLoginSid, principal, loginTime, clientConnectionId);
    internal static OwnSqlSession Decode(object[] row, Guid clientConnectionId)
    {
        try
        {
            if (row.Length != 15 || clientConnectionId == Guid.Empty) throw SessionFailure.Invalid();
            int Int(int i) => row[i] is int value ? value : throw SessionFailure.Invalid();
            string Text(int i)
            {
                if (row[i] is not string value || value.Length is < 1 or > 128 || value.Any(c => c < 32)) throw SessionFailure.Invalid();
                _ = new UTF8Encoding(false, true).GetByteCount(value); return value;
            }
            string Sid(int i) => row[i] is byte[] value && value.Length is >= 1 and <= 85
                ? Convert.ToHexString(value).ToLowerInvariant() : throw SessionFailure.Invalid();
            int session = Int(0), database = Int(2);
            if (session is < 1 or > 32767 || database < 1 || Int(12) != 0 || Int(13) is not (0 or 1) || Int(14) != 0)
                throw SessionFailure.Invalid();
            string? nonce = row[1] switch
            {
                DBNull => null,
                byte[] value when value.Length == 0 => null,
                byte[] value when value.Length == 32 && value.Any(b => b != 0) => Convert.ToHexString(value).ToLowerInvariant(),
                _ => throw SessionFailure.Invalid()
            };
            if (row[11] is not DateTime login || login.Kind != DateTimeKind.Unspecified || login <= new DateTime(1900, 1, 1) || login.Ticks % 10 != 0)
                throw SessionFailure.Invalid();
            return new(session, nonce, database, Text(3), Text(4), Sid(5), Text(6), Sid(7),
                new(Int(8), Text(9), Sid(10)), login, clientConnectionId);
        }
        catch (Exception) { throw SessionFailure.Invalid(); }
    }
    internal void RequireCredentials(string database, string username)
    {
        if (DatabaseName != database || LoginName != username || OriginalLoginName != username || LoginSid != OriginalLoginSid)
            throw SessionFailure.Invalid();
    }
    internal void RequireSameIncarnation(OwnSqlSession before)
    {
        if (SessionId != before.SessionId || DatabaseId != before.DatabaseId || DatabaseName != before.DatabaseName ||
            LoginName != before.LoginName || LoginSid != before.LoginSid || OriginalLoginName != before.OriginalLoginName ||
            OriginalLoginSid != before.OriginalLoginSid || Principal != before.Principal || LoginTime != before.LoginTime ||
            ClientConnectionId != before.ClientConnectionId) throw SessionFailure.Invalid();
    }
    /// <summary>Do not include observed identifiers in accidental diagnostic formatting.</summary>
    public override string ToString() => "OwnSqlSession [REDACTED]";
}

internal static class SessionFailure
{
    internal static InvalidDataException Invalid() => new("writer.session_invalid");
    internal static InvalidOperationException Failed() => new("writer.session_failed");
}
