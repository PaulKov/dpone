using System.Text.Json;
using Dpone.SqlClient.Input;
using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Session;

/// <summary>Existing closed Python TdsAttemptError vocabulary; never free-form SDK diagnostics.</summary>
public enum SessionResultError
{
    /// <summary>Original startup deadline elapsed.</summary>
    StartupTimeout,
    /// <summary>Original operation deadline elapsed.</summary>
    OperationTimeout,
    /// <summary>Admitted resource limit prevented completion.</summary>
    ResourceLimit,
    /// <summary>Native decoding failed.</summary>
    Decoder,
    /// <summary>Driver operation failed.</summary>
    Driver,
    /// <summary>Constraint enforcement failed.</summary>
    Constraint,
    /// <summary>Required permission was unavailable.</summary>
    Permission,
    /// <summary>Connection operation failed.</summary>
    Connection,
    /// <summary>Process outcome could not be established.</summary>
    ProcessUnknown,
    /// <summary>Original ownership could not be established.</summary>
    Ownership,
    /// <summary>Fence validation failed.</summary>
    Fencing,
    /// <summary>Verification failed.</summary>
    Verification,
    /// <summary>Protocol validation failed.</summary>
    Protocol,
    /// <summary>Cleanup failed.</summary>
    Cleanup,
    /// <summary>Coordinator continuity was lost.</summary>
    CoordinatorLost
}

/// <summary>
/// Exactly one launch/attempt result and optional accepted grant. Callers retain
/// raw bytes before parsing and independently observe channel EOF, process exit,
/// remote settlement and exact stage verification. No result authorizes replay.
/// </summary>
public sealed class SessionResult
{
    private const int Limit = 16384;
    private const string EmptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    /// <summary>Original canonical launch binding.</summary>
    public string LaunchSha256 { get; }
    /// <summary>Original complete attempt binding shared by wrapper and nested result.</summary>
    public string AttemptSha256 { get; }
    /// <summary>Accepted grant, or null for preacceptance error or canonical empty success.</summary>
    public string? GrantId { get; }
    /// <summary>Complete original input receipt only on success; never partial authority.</summary>
    public InputReceipt? Receipt { get; }
    /// <summary>Closed diagnostic only on failure.</summary>
    public SessionResultError? Error { get; }
    /// <summary>Construct a structurally valid body; this does not validate against external expectations.</summary>
    public SessionResult(string launchSha256, string attemptSha256, string? grantId, InputReceipt? receipt, SessionResultError? error)
    {
        Hash(launchSha256); Hash(attemptSha256);
        if (grantId is not null) Uuid(grantId);
        if (receipt is not null)
        {
            Input(receipt);
            if (error is not null || (receipt.Rows == 0) != (grantId is null)) throw Invalid();
        }
        else if (error is null) throw Invalid();
        if (error is not null) _ = ErrorName(error.Value);
        (LaunchSha256, AttemptSha256, GrantId, Receipt, Error) = (launchSha256, attemptSha256, grantId, receipt, error);
    }
    /// <summary>Canonical unframed wrapper with the existing nested result body, never a second frame.</summary>
    public byte[] Encode()
    {
        object? receipt = Receipt is null ? null : new { rows=Receipt.Rows, encoded_bytes=Receipt.Bytes, file_sha256=Receipt.Sha256 };
        byte[] bytes = JsonWire.Canonical(JsonSerializer.SerializeToElement(new {
            schema_version=1, launch_sha256=LaunchSha256, attempt_sha256=AttemptSha256, grant_id=GrantId,
            result=new { schema_version=1, status=Receipt is null?"error":"success", attempt_sha256=AttemptSha256,
                input_eof=Receipt is not null, receipt, error=Error is null?null:ErrorName(Error.Value) } }));
        if (bytes.Length > Limit) throw Invalid();
        return bytes;
    }
    /// <summary>
    /// Validate bytes against independently supplied original launch/input/grant.
    /// A null reported grant on error is permitted even when parent intent exists;
    /// it does not prove the absence of a live remote session or grant acceptance.
    /// </summary>
    public static SessionResult Decode(byte[] body, StartupLaunch launch, InputReceipt expectedInput, string? expectedGrantId)
    {
        try
        {
            if (body is null || body.Length is < 1 or > Limit || launch is null) throw Invalid();
            Input(expectedInput);
            if (expectedGrantId is not null) { Uuid(expectedGrantId); if (expectedInput.Rows == 0) throw Invalid(); }
            using var doc=JsonWire.Document(body); var value=doc.RootElement;
            JsonWire.Shape(value,"schema_version","launch_sha256","attempt_sha256","grant_id","result");
            JsonWire.Integer(value,"schema_version",1,1);
            string? grant=value.GetProperty("grant_id").ValueKind==JsonValueKind.Null?null:JsonWire.Uuid(value,"grant_id");
            var nested=value.GetProperty("result");
            JsonWire.Shape(nested,"schema_version","status","attempt_sha256","input_eof","receipt","error");
            JsonWire.Integer(nested,"schema_version",1,1);
            InputReceipt? receipt=null;
            var rawReceipt=nested.GetProperty("receipt");
            if(rawReceipt.ValueKind!=JsonValueKind.Null)
            {
                JsonWire.Shape(rawReceipt,"rows","encoded_bytes","file_sha256");
                receipt=new(JsonWire.Integer(rawReceipt,"rows"),JsonWire.Integer(rawReceipt,"encoded_bytes"),JsonWire.Hash(rawReceipt,"file_sha256"));
            }
            SessionResultError? error=nested.GetProperty("error").ValueKind==JsonValueKind.Null?null:ParseError(JsonWire.Text(nested,"error"));
            if(JsonWire.Hash(nested,"attempt_sha256")!=launch.AttemptSha256 || JsonWire.Text(nested,"status")!=(receipt is null?"error":"success") ||
                nested.GetProperty("input_eof").ValueKind!=(receipt is null?JsonValueKind.False:JsonValueKind.True) ||
                receipt is not null && receipt!=expectedInput) throw Invalid();
            var result=new SessionResult(JsonWire.Hash(value,"launch_sha256"),JsonWire.Hash(value,"attempt_sha256"),grant,receipt,error);
            if(result.LaunchSha256!=launch.Digest || result.AttemptSha256!=launch.AttemptSha256 || grant is not null && grant!=expectedGrantId) throw Invalid();
            return result;
        }
        catch(Exception) { throw Invalid(); }
    }
    private static void Input(InputReceipt value)
    {
        if(value is null || value.Rows<0 || value.Bytes<0 || (value.Rows==0)!=(value.Bytes==0) || value.Rows==0 && value.Sha256!=EmptyHash) throw Invalid();
        Hash(value.Sha256);
    }
    private static void Hash(string value)
    {
        if(value is null || value.Length!=64 || value.Any(c=>!(c is >= '0' and <= '9' or >= 'a' and <= 'f'))) throw Invalid();
    }
    private static void Uuid(string value)
    {
        if(!Guid.TryParseExact(value,"D",out var parsed) || parsed==Guid.Empty || parsed.ToString("D")!=value) throw Invalid();
    }
    private static InvalidDataException Invalid() => new("session.result_invalid");
    private static SessionResultError ParseError(string value)
    {
        foreach(var candidate in Enum.GetValues<SessionResultError>()) if(ErrorName(candidate)==value) return candidate;
        throw Invalid();
    }
    private static string ErrorName(SessionResultError value) => value switch {
        SessionResultError.StartupTimeout=>"startup_timeout", SessionResultError.OperationTimeout=>"operation_timeout",
        SessionResultError.ResourceLimit=>"resource_limit", SessionResultError.Decoder=>"decoder", SessionResultError.Driver=>"driver",
        SessionResultError.Constraint=>"constraint", SessionResultError.Permission=>"permission", SessionResultError.Connection=>"connection",
        SessionResultError.ProcessUnknown=>"process_unknown", SessionResultError.Ownership=>"ownership", SessionResultError.Fencing=>"fencing",
        SessionResultError.Verification=>"verification", SessionResultError.Protocol=>"protocol", SessionResultError.Cleanup=>"cleanup",
        SessionResultError.CoordinatorLost=>"coordinator_lost", _=>throw Invalid() };
}
