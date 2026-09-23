using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Session;

/// <summary>Strict unframed session-control codecs; no clocks, I/O, SQL observations or grant side effects.</summary>
public static class SessionControlCodec
{
    /// <summary>Maximum complete unframed control body.</summary>
    public const int MaxControlBytes = 16384;
    internal static InvalidDataException Invalid() => new("session.control_invalid");
    internal static void Positive(long value) { if (value < 1) throw Invalid(); }
    internal static void SessionId(int value) { if (value is < 1 or > 32767) throw Invalid(); }
    internal static void Hash(string value)
    {
        if (value is null || value.Length != 64 || value.Any(c => !(c is >= '0' and <= '9' or >= 'a' and <= 'f'))) throw Invalid();
    }
    internal static void Nonce(string value) { Hash(value); if (value.All(c => c == '0')) throw Invalid(); }
    internal static void Uuid(string value)
    {
        if (!Guid.TryParseExact(value, "D", out var parsed) || parsed == Guid.Empty || parsed.ToString("D") != value) throw Invalid();
    }
    internal static void Owner(string value)
    {
        if (value is null) throw Invalid();
        try { _ = new UTF8Encoding(false, true).GetByteCount(value); }
        catch (EncoderFallbackException) { throw Invalid(); }
        int count = 0;
        foreach (Rune rune in value.EnumerateRunes()) { if (rune.Value < 32) throw Invalid(); count++; }
        if (count is < 1 or > 256) throw Invalid();
    }
    internal static void Timestamp(string value)
    {
        const string format = "yyyy-MM-dd'T'HH:mm:ss.ffffff";
        if (value is null || value.Length != 26 || !DateTime.TryParseExact(value, format, CultureInfo.InvariantCulture,
            DateTimeStyles.None, out DateTime parsed) || parsed <= new DateTime(1900, 1, 1) ||
            parsed.ToString(format, CultureInfo.InvariantCulture) != value) throw Invalid();
    }
    internal static void Current(StartupLaunch launch, long nowNs)
    {
        if (launch is null || nowNs < 0 || nowNs >= launch.OperationDeadlineNs) throw Invalid();
    }
    private static JsonDocument Body(byte[] body)
    {
        if (body is null || body.Length is < 1 or > MaxControlBytes) throw Invalid();
        return JsonWire.Document(body);
    }
    /// <summary>Decode the closed child locator; callers must independently bind and observe it.</summary>
    public static SessionAnnouncement DecodeAnnouncement(byte[] body)
    {
        try
        {
            using var doc = Body(body); var v = doc.RootElement;
            JsonWire.Shape(v, "schema_version", "launch_sha256", "attempt_sha256", "session_id", "nonce");
            JsonWire.Integer(v, "schema_version", 1, 1);
            return new(JsonWire.Hash(v, "launch_sha256"), JsonWire.Hash(v, "attempt_sha256"),
                (int)JsonWire.Integer(v, "session_id", 1, 32767), JsonWire.Hash(v, "nonce"));
        }
        catch (Exception) { throw Invalid(); }
    }
    /// <summary>Decode exact nested identities and canonical timestamps without granting permission.</summary>
    public static BulkGrant DecodeGrant(byte[] body)
    {
        try
        {
            using var doc = Body(body); var v = doc.RootElement;
            JsonWire.Shape(v, "schema_version", "grant_id", "launch_sha256", "attempt_sha256", "ownership", "process", "object_identity",
                "input_binding_sha256", "build_sha256", "remote_session", "writer_observation_sha256", "operation_deadline_ns", "resolved_database_principal");
            JsonWire.Integer(v, "schema_version", 1, 1);
            var owner = v.GetProperty("ownership"); JsonWire.Shape(owner, "owner", "fence", "supervisor_id");
            var process = v.GetProperty("process"); JsonWire.Shape(process, "host_sha256", "boot_id", "pid", "start_ticks");
            var stage = v.GetProperty("object_identity"); JsonWire.Shape(stage, "object_id", "fingerprint");
            var principal = v.GetProperty("resolved_database_principal"); JsonWire.Shape(principal, "principal_id", "name", "sid");
            return new(JsonWire.Uuid(v, "grant_id"), JsonWire.Hash(v, "launch_sha256"), JsonWire.Hash(v, "attempt_sha256"),
                new(JsonWire.Text(owner, "owner"), JsonWire.Integer(owner, "fence", 1), JsonWire.Uuid(owner, "supervisor_id")),
                new(JsonWire.Hash(process, "host_sha256"), JsonWire.Uuid(process, "boot_id"), (int)JsonWire.Integer(process, "pid", 1, int.MaxValue), JsonWire.Integer(process, "start_ticks")),
                new((int)JsonWire.Integer(stage, "object_id", 1, int.MaxValue), JsonWire.Hash(stage, "fingerprint")),
                JsonWire.Hash(v, "input_binding_sha256"), JsonWire.Hash(v, "build_sha256"), Remote(v.GetProperty("remote_session")),
                JsonWire.Hash(v, "writer_observation_sha256"), JsonWire.Integer(v, "operation_deadline_ns", 1),
                new((int)JsonWire.Integer(principal, "principal_id", 1, int.MaxValue), JsonWire.Text(principal, "name"), JsonWire.Text(principal, "sid")));
        }
        catch (Exception) { throw Invalid(); }
    }
    private static RemoteSessionIdentity Remote(JsonElement v)
    {
        if (JsonWire.Canonical(v).Length > 1024) throw Invalid();
        JsonWire.Shape(v, "schema", "connection_id", "session_id", "connect_time", "login_time", "nonce", "authority_sha256");
        if (JsonWire.Text(v, "schema") != "dpone.tds.remote-session.v1") throw Invalid();
        return new(JsonWire.Uuid(v, "connection_id"), (int)JsonWire.Integer(v, "session_id", 1, 32767),
            JsonWire.Text(v, "connect_time"), JsonWire.Text(v, "login_time"), JsonWire.Hash(v, "nonce"), JsonWire.Hash(v, "authority_sha256"));
    }
    private static byte[] Encode(object value)
    {
        byte[] body = JsonWire.Canonical(JsonSerializer.SerializeToElement(value));
        if (body.Length > MaxControlBytes) throw Invalid();
        return body;
    }
    /// <summary>Encode canonical unframed locator bytes; transport owns send-once and EOF.</summary>
    public static byte[] EncodeAnnouncement(SessionAnnouncement value)
    {
        if (value is null) throw Invalid();
        return Encode(new { schema_version = 1, launch_sha256 = value.LaunchSha256, attempt_sha256 = value.AttemptSha256,
            session_id = value.SessionId, nonce = value.Nonce });
    }
    /// <summary>Encode the original closed grant using the shared Python-compatible canonical convention.</summary>
    public static byte[] EncodeGrant(BulkGrant v)
    {
        if (v is null) throw Invalid();
        return Encode(new { schema_version = 1, grant_id = v.GrantId, launch_sha256 = v.LaunchSha256, attempt_sha256 = v.AttemptSha256,
            ownership = new { owner = v.Ownership.Owner, fence = v.Ownership.Fence, supervisor_id = v.Ownership.SupervisorId },
            process = new { host_sha256 = v.Process.HostSha256, boot_id = v.Process.BootId, pid = v.Process.Pid, start_ticks = v.Process.StartTicks },
            object_identity = new { object_id = v.ObjectIdentity.ObjectId, fingerprint = v.ObjectIdentity.Fingerprint },
            input_binding_sha256 = v.InputBindingSha256, build_sha256 = v.BuildSha256,
            remote_session = new { schema = "dpone.tds.remote-session.v1", connection_id = v.RemoteSession.ConnectionId,
                session_id = v.RemoteSession.SessionId, connect_time = v.RemoteSession.ConnectTime, login_time = v.RemoteSession.LoginTime,
                nonce = v.RemoteSession.Nonce, authority_sha256 = v.RemoteSession.AuthoritySha256 },
            writer_observation_sha256 = v.WriterObservationSha256, operation_deadline_ns = v.OperationDeadlineNs,
            resolved_database_principal = new { principal_id = v.ResolvedPrincipal.PrincipalId, name = v.ResolvedPrincipal.Name, sid = v.ResolvedPrincipal.Sid } });
    }
    /// <summary>Semantic domain digest; never substitute for an immutable evidence artifact byte hash.</summary>
    public static string GrantDigest(BulkGrant value) => Convert.ToHexString(SHA256.HashData(
        Encoding.ASCII.GetBytes("dpone.sqlclient.bulk-grant.v1\0").Concat(EncodeGrant(value)).ToArray())).ToLowerInvariant();
}
