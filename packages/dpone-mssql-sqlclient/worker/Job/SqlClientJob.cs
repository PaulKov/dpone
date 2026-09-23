using System.Text.Json;
using Dpone.SqlClient.Startup;
using Dpone.SqlClient.Session;

namespace Dpone.SqlClient.Job;

/// <summary>Trusted transport settings supplied independently by composition; not the full policy identity.</summary>
public sealed record JobTransportPolicy(string Backend, string Input, int BatchRows, int MaxInputBatchBytes, long AddressSpaceBytes);

/// <summary>Closed immutable memory-only job; structural admission grants no SQL, file, process or delivery effects.</summary>
public sealed class SqlClientJob
{
    /// <summary>Maximum complete unframed job body, checked before JSON parsing.</summary>
    public const int MaxJobBytes = 1 << 20;
    /// <summary>Original launch digest.</summary>
    public string LaunchSha256 { get; }
    /// <summary>Complete immutable original attempt.</summary>
    public JobAttemptIdentity Identity { get; }
    /// <summary>Original owner and fence; not a current fence observation.</summary>
    public SessionOwnership Ownership { get; }
    /// <summary>Original stage incarnation; no SQL discovery occurs.</summary>
    public SessionObjectIdentity ObjectIdentity { get; }
    /// <summary>Path-free declared native input.</summary>
    public JobInputDescriptor Input { get; }
    /// <summary>Explicit rows or arrow representation.</summary>
    public string InputMode { get; }
    /// <summary>Positive decoded batch row bound.</summary>
    public int BatchRows { get; }
    /// <summary>Decoded retained input byte budget.</summary>
    public int MaxInputBatchBytes { get; }
    /// <summary>Private connection material, absent for empty input; never log or hash.</summary>
    public SqlClientCredentials? Credentials { get; }
    /// <summary>Original nonzero session nonce, absent for empty input.</summary>
    public string? SessionNonce { get; }
    /// <summary>Secret-free semantic digest; does not assert credential/principal identity.</summary>
    public string BindingDigest => JsonWire.Digest("dpone.sqlclient.job-binding.v1\0", JsonSerializer.SerializeToElement(Projection(true)));
    private SqlClientJob(JsonElement v)
    {
        JsonWire.Shape(v, "schema_version", "launch_sha256", "identity", "ownership", "object_identity", "input", "input_mode", "batch_rows", "max_input_batch_bytes", "credentials", "session_nonce");
        JsonWire.Integer(v, "schema_version", 1, 1);
        LaunchSha256 = JsonWire.Hash(v, "launch_sha256");
        Identity = new(v.GetProperty("identity"));
        var owner = v.GetProperty("ownership"); JsonWire.Shape(owner, "owner", "fence", "supervisor_id");
        Ownership = new(JsonWire.Text(owner, "owner"), JsonWire.Integer(owner, "fence", 1), JsonWire.Uuid(owner, "supervisor_id"));
        var stage = v.GetProperty("object_identity"); JsonWire.Shape(stage, "object_id", "fingerprint");
        ObjectIdentity = new((int)JsonWire.Integer(stage, "object_id", 1, int.MaxValue), JsonWire.Hash(stage, "fingerprint"));
        Input = new(v.GetProperty("input"));
        InputMode = JsonWire.Text(v, "input_mode");
        if (InputMode is not ("rows" or "arrow")) throw JobScalars.Invalid();
        BatchRows = (int)JsonWire.Integer(v, "batch_rows", 1, 65536);
        MaxInputBatchBytes = (int)JsonWire.Integer(v, "max_input_batch_bytes", 1 << 20, 256 << 20);
        Credentials = v.GetProperty("credentials").ValueKind == JsonValueKind.Null ? null : new(v.GetProperty("credentials"));
        SessionNonce = JobScalars.OptionalText(v, "session_nonce");
        if (Identity.FileSha256 != Input.Native.Expected.Sha256) throw JobScalars.Invalid();
        if (Input.Native.Expected.Rows == 0)
        {
            if (Credentials is not null || SessionNonce is not null) throw JobScalars.Invalid();
        }
        else
        {
            if (Credentials is null || Credentials.Database != Identity.Database || SessionNonce is null) throw JobScalars.Invalid();
            SessionControlCodec.Nonce(SessionNonce);
        }
    }
    // Build only nonsecret fields here. Never serialize credentials and then remove them.
    private Dictionary<string, object?> Projection(bool binding)
    {
        var fields = new Dictionary<string, object?>
        {
            ["schema_version"] = 1, ["launch_sha256"] = LaunchSha256, ["identity"] = Identity.Wire,
            ["ownership"] = new { owner = Ownership.Owner, fence = Ownership.Fence, supervisor_id = Ownership.SupervisorId },
            ["object_identity"] = new { object_id = ObjectIdentity.ObjectId, fingerprint = ObjectIdentity.Fingerprint },
            ["input"] = Input.Wire, ["input_mode"] = InputMode, ["batch_rows"] = BatchRows,
            ["max_input_batch_bytes"] = MaxInputBatchBytes, ["session_nonce"] = SessionNonce
        };
        if (binding) fields["credentials_present"] = Credentials is not null;
        return fields;
    }
    /// <summary>Explicit nonsecret canonical binding projection, excluding the entire credentials object.</summary>
    public byte[] BindingBytes() => JsonWire.Canonical(JsonSerializer.SerializeToElement(Projection(true)));
    /// <summary>Secret canonical bytes for one private inherited channel only. Never persist, log or hash these bytes.</summary>
    public byte[] Encode()
    {
        var fields = Projection(false); fields["credentials"] = Credentials?.Wire();
        byte[] body = JsonWire.Canonical(JsonSerializer.SerializeToElement(fields));
        if (body.Length > MaxJobBytes) throw JobScalars.Invalid();
        return body;
    }
    /// <summary>Require a closed bounded body and exact nested scalar types; all errors exclude supplied values.</summary>
    public static SqlClientJob Parse(byte[] body)
    {
        try
        {
            if (body is null || body.Length is < 1 or > MaxJobBytes) throw JobScalars.Invalid();
            using var doc = JsonWire.Document(body);
            return new(doc.RootElement);
        }
        catch (Exception) { throw JobScalars.Invalid(); }
    }
    /// <summary>
    /// Compare every binding with trusted originals, never expectations copied from the untrusted Job.
    /// Composition retains full policy admission and supplies an explicit synthetic TLS exception when authorized.
    /// This comparison does not perform any runtime, session-grant, input-observation or SQL effects.
    /// </summary>
    public void Validate(StartupLaunch launch, SessionOwnership ownership, SessionObjectIdentity objectIdentity,
        JobTransportPolicy policy, string? sessionNonce, string? tlsProfile, bool allowDisposableTest, long nowNs)
    {
        if (launch is null || ownership is null || objectIdentity is null || policy is null || nowNs < 0 ||
            LaunchSha256 != launch.Digest || Identity.Digest != launch.AttemptSha256 || Input.Digest != launch.InputBindingSha256 ||
            Input.Fd != launch.Descriptors.Input || Ownership != ownership || ObjectIdentity != objectIdentity ||
            policy.Backend != "mssql_sqlclient" || InputMode != policy.Input || BatchRows != policy.BatchRows ||
            MaxInputBatchBytes != policy.MaxInputBatchBytes || launch.AddressSpaceBytes != policy.AddressSpaceBytes ||
            SessionNonce != sessionNonce || Credentials?.TlsProfile != tlsProfile ||
            (Credentials?.TlsProfile == "disposable_test" && !allowDisposableTest) || nowNs >= launch.OperationDeadlineNs)
            throw JobScalars.Invalid();
    }
    /// <summary>Redacted diagnostic representation, including nested credentials.</summary>
    public override string ToString() => "SqlClientJob [REDACTED]";
}
