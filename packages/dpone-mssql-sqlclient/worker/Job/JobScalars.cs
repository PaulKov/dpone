using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Dpone.SqlClient.Startup;

namespace Dpone.SqlClient.Job;

internal static class JobScalars
{
    internal static InvalidDataException Invalid() => new("mssql_native.sqlclient_job_invalid");
    internal static string Text(JsonElement v, string key, int limit = 256, bool identifier = false)
    {
        string text = JsonWire.Text(v, key);
        _ = new UTF8Encoding(false, true).GetByteCount(text);
        if (text.Length == 0 || text.EnumerateRunes().Count() > limit || text.Any(c => c < 32) ||
            (identifier && text.Length > 128)) throw Invalid();
        return text;
    }
    internal static bool Boolean(JsonElement v, string key) => v.GetProperty(key).ValueKind switch
    { JsonValueKind.True => true, JsonValueKind.False => false, _ => throw Invalid() };
    internal static int? OptionalInteger(JsonElement v, string key) => v.GetProperty(key).ValueKind == JsonValueKind.Null
        ? null : (int)JsonWire.Integer(v, key, 0, 255);
    internal static string? OptionalText(JsonElement v, string key) => v.GetProperty(key).ValueKind == JsonValueKind.Null
        ? null : JsonWire.Text(v, key);
    internal static ulong Unsigned(JsonElement v, string key)
    {
        var field = v.GetProperty(key);
        if (field.ValueKind != JsonValueKind.Number || !field.TryGetUInt64(out ulong number)) throw Invalid();
        return number;
    }
}

/// <summary>Immutable memory-only connection material; never log or persist its fields or encoded Job.</summary>
public sealed class SqlClientCredentials
{
    /// <summary>Explicit admitted host; no connection-string syntax.</summary>
    public string Host { get; }
    /// <summary>Explicit TCP port.</summary>
    public int Port { get; }
    /// <summary>Original database identifier.</summary>
    public string Database { get; }
    /// <summary>Restricted writer principal.</summary>
    public string Username { get; }
    /// <summary>Secret for private one-shot delivery only; managed strings are not zeroizable.</summary>
    public string Password { get; }
    /// <summary>Explicit verified or disposable_test profile; representation grants no TLS exception.</summary>
    public string TlsProfile { get; }
    internal SqlClientCredentials(JsonElement v)
    {
        JsonWire.Shape(v, "host", "port", "database", "username", "password", "tls_profile");
        Host = JsonWire.Text(v, "host");
        if (!Regex.IsMatch(Host, @"\A[A-Za-z0-9.\-:\[\]]{1,255}\z")) throw JobScalars.Invalid();
        Port = (int)JsonWire.Integer(v, "port", 1, 65535);
        Database = JobScalars.Text(v, "database", 128, true);
        Username = JobScalars.Text(v, "username", 128, true);
        Password = JsonWire.Text(v, "password");
        if (Password.Length == 0 || Password.Contains('\0') || new UTF8Encoding(false, true).GetByteCount(Password) > 16384)
            throw JobScalars.Invalid();
        TlsProfile = JsonWire.Text(v, "tls_profile");
        if (TlsProfile is not ("verified" or "disposable_test")) throw JobScalars.Invalid();
    }
    internal object Wire() => new { host = Host, port = Port, database = Database, username = Username, password = Password, tls_profile = TlsProfile };
    /// <summary>Never expose credential fields through diagnostics.</summary>
    public override string ToString() => "SqlClientCredentials [REDACTED]";
}

/// <summary>Immutable full caller-owned attempt identity; its policy digest is never replaced by a transport hash.</summary>
public sealed class JobAttemptIdentity
{
    internal JsonElement Wire { get; }
    /// <summary>Canonical complete attempt SHA256, matching Python without a domain prefix.</summary>
    public string Digest { get; }
    /// <summary>Exact database identifier.</summary>
    public string Database { get; }
    /// <summary>Exact schema identifier.</summary>
    public string Schema { get; }
    /// <summary>Exact table identifier.</summary>
    public string Table { get; }
    /// <summary>Expected complete source byte digest.</summary>
    public string FileSha256 { get; }
    internal JobAttemptIdentity(JsonElement v)
    {
        JsonWire.Shape(v, "target_key", "run_id", "ordinal", "attempt", "plan_sha256", "policy_sha256", "implementation_sha256", "file_sha256", "database", "schema", "table", "owner_binding");
        JobScalars.Text(v, "target_key"); JobScalars.Text(v, "run_id");
        JsonWire.Integer(v, "ordinal"); JsonWire.Integer(v, "attempt", 0, 2);
        foreach (string name in new[] { "plan_sha256", "policy_sha256", "implementation_sha256", "owner_binding" }) JsonWire.Hash(v, name);
        FileSha256 = JsonWire.Hash(v, "file_sha256");
        Database = JobScalars.Text(v, "database", 128, true);
        Schema = JobScalars.Text(v, "schema", 128, true);
        Table = JobScalars.Text(v, "table", 128, true);
        Wire = v.Clone(); Digest = JsonWire.Digest("", Wire);
    }
}
