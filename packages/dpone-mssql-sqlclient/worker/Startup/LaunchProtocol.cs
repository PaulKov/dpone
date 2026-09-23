using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Dpone.SqlClient.Startup;

/// <summary>Observed Linux process incarnation, independent of numeric PID reuse.</summary>
public sealed record StartupProcessIdentity(string HostSha256, string BootId, int Pid, long StartTicks);
/// <summary>Six admitted inherited roles; the Python bootstrap gate is already closed.</summary>
public sealed record StartupDescriptors(int Startup, int Credentials, int Session, int Grant, int Result, int Input)
{
    internal int[] All => new[] { Startup, Credentials, Session, Grant, Result, Input };
}

/// <summary>Strict non-secret launch permission; never a credential or SQL grant.</summary>
public sealed class StartupLaunch
{
    /// <summary>Canonical domain-separated launch digest.</summary>
    public string Digest { get; }
    /// <summary>Trusted deployment-manifest digest admitted independently by the parent.</summary>
    public string BuildSha256 { get; }
    /// <summary>Full immutable attempt binding retained for Session admission.</summary>
    public string AttemptSha256 { get; }
    /// <summary>Admitted native input descriptor digest retained for Session admission.</summary>
    public string InputBindingSha256 { get; }
    /// <summary>Expected process incarnation acquired by the parent before exec.</summary>
    public StartupProcessIdentity Process { get; }
    /// <summary>Original creating parent PID, not proof of its creating thread liveness.</summary>
    public int ParentPid { get; }
    /// <summary>Linux CLOCK_MONOTONIC original startup deadline, in nanoseconds.</summary>
    public long StartupDeadlineNs { get; }
    /// <summary>Original operation deadline, independent of termination allowance.</summary>
    public long OperationDeadlineNs { get; }
    /// <summary>Expected effective soft and hard address-space ceiling.</summary>
    public long AddressSpaceBytes { get; }
    /// <summary>Unique descriptor roles, each at least3.</summary>
    public StartupDescriptors Descriptors { get; }
    private StartupLaunch(JsonElement value)
    {
        JsonWire.Shape(value, "schema_version", "nonce", "attempt_sha256", "build_sha256", "input_binding_sha256", "process", "parent_pid", "startup_deadline_ns", "operation_deadline_ns", "address_space_bytes", "descriptors");
        JsonWire.Integer(value, "schema_version", 1, 1);
        JsonWire.Uuid(value, "nonce");
        AttemptSha256 = JsonWire.Hash(value, "attempt_sha256");
        InputBindingSha256 = JsonWire.Hash(value, "input_binding_sha256");
        BuildSha256 = JsonWire.Hash(value, "build_sha256");
        JsonElement process = value.GetProperty("process");
        JsonWire.Shape(process, "host_sha256", "boot_id", "pid", "start_ticks");
        Process = new(JsonWire.Hash(process, "host_sha256"), JsonWire.Uuid(process, "boot_id"),
            (int)JsonWire.Integer(process, "pid", 1, int.MaxValue), JsonWire.Integer(process, "start_ticks", 0));
        ParentPid = (int)JsonWire.Integer(value, "parent_pid", 1, int.MaxValue);
        if (ParentPid == Process.Pid) throw new InvalidDataException("startup.parent_identity");
        StartupDeadlineNs = JsonWire.Integer(value, "startup_deadline_ns", 1);
        OperationDeadlineNs = JsonWire.Integer(value, "operation_deadline_ns", 1);
        AddressSpaceBytes = JsonWire.Integer(value, "address_space_bytes", 8L * 1024 * 1024 * 1024, 16L * 1024 * 1024 * 1024);
        JsonElement descriptors = value.GetProperty("descriptors");
        JsonWire.Shape(descriptors, "startup", "credentials", "session", "grant", "result", "input");
        int Fd(string name) => (int)JsonWire.Integer(descriptors, name, 3, int.MaxValue);
        Descriptors = new(Fd("startup"), Fd("credentials"), Fd("session"), Fd("grant"), Fd("result"), Fd("input"));
        if (Descriptors.All.Distinct().Count() != 6) throw new InvalidDataException("startup.descriptor_alias");
        Digest = JsonWire.Digest("dpone.sqlclient.launch.v1\0", value);
    }
    /// <summary>Decode closed wire shape, duplicate fields, exact integers and all bounds.</summary>
    public static StartupLaunch Parse(byte[] body)
    {
        try
        {
            if (body.Length is < 1 or > 16384) throw new InvalidDataException("startup.launch_size");
            using JsonDocument doc = JsonWire.Document(body);
            return new StartupLaunch(doc.RootElement);
        }
        catch (Exception) { throw new InvalidDataException("startup.launch_invalid"); }
    }
}

internal static class JsonWire
{
    internal static JsonDocument Document(byte[] bytes)
    {
        JsonDocument document = JsonDocument.Parse(bytes);
        try { Unique(document.RootElement); return document; }
        catch { document.Dispose(); throw; }
    }
    private static void Unique(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (JsonProperty property in value.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new InvalidDataException("startup.duplicate_field");
                Unique(property.Value);
            }
        }
        else if (value.ValueKind == JsonValueKind.Array)
            foreach (JsonElement item in value.EnumerateArray()) Unique(item);
    }
    internal static void Shape(JsonElement value, params string[] names)
    {
        if (value.ValueKind != JsonValueKind.Object) throw new InvalidDataException("startup.object");
        var found = new HashSet<string>(StringComparer.Ordinal);
        foreach (JsonProperty property in value.EnumerateObject())
            if (!names.Contains(property.Name, StringComparer.Ordinal) || !found.Add(property.Name))
                throw new InvalidDataException("startup.fields");
        if (found.Count != names.Length) throw new InvalidDataException("startup.fields");
    }
    internal static string Text(JsonElement value, string name)
    {
        JsonElement field = value.GetProperty(name);
        if (field.ValueKind != JsonValueKind.String) throw new InvalidDataException("startup.text");
        return field.GetString()!;
    }
    internal static long Integer(JsonElement value, string name, long min = 0, long max = long.MaxValue)
    {
        JsonElement field = value.GetProperty(name);
        if (field.ValueKind != JsonValueKind.Number || !field.TryGetInt64(out long number) || number < min || number > max)
            throw new InvalidDataException("startup.integer");
        return number;
    }
    internal static string Hash(JsonElement value, string name)
    {
        string text = Text(value, name);
        if (text.Length != 64 || text.Any(c => !(c is >= '0' and <= '9' or >= 'a' and <= 'f')))
            throw new InvalidDataException("startup.hash");
        return text;
    }
    internal static string Uuid(JsonElement value, string name)
    {
        string text = Text(value, name);
        if (!Guid.TryParseExact(text, "D", out Guid parsed) || parsed == Guid.Empty || parsed.ToString("D") != text)
            throw new InvalidDataException("startup.uuid");
        return text;
    }
    internal static byte[] Canonical(JsonElement value)
    {
        using var bytes = new MemoryStream();
        using (var writer = new Utf8JsonWriter(bytes, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping })) Write(writer, value);
        return bytes.ToArray();
    }
    private static void Write(Utf8JsonWriter writer, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (JsonProperty property in value.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal))
                { writer.WritePropertyName(property.Name); Write(writer, property.Value); }
                writer.WriteEndObject(); break;
            case JsonValueKind.Array:
                writer.WriteStartArray(); foreach (JsonElement item in value.EnumerateArray()) Write(writer, item);
                writer.WriteEndArray(); break;
            case JsonValueKind.String: writer.WriteRawValue(CanonicalString(value.GetString()!), skipInputValidation: false); break;
            case JsonValueKind.Number:
                if (value.TryGetInt64(out long signed)) writer.WriteNumberValue(signed);
                else if (value.TryGetUInt64(out ulong unsigned)) writer.WriteNumberValue(unsigned);
                else throw new InvalidDataException("startup.integer");
                break;
            case JsonValueKind.True: writer.WriteBooleanValue(true); break;
            case JsonValueKind.False: writer.WriteBooleanValue(false); break;
            case JsonValueKind.Null: writer.WriteNullValue(); break;
            default: throw new InvalidDataException("startup.json");
        }
    }
    // Python ensure_ascii=False emits supplementary Unicode as literal UTF-8.
    // System.Text.Json's relaxed encoder still escapes some valid scalars, so
    // emit only JSON-required escapes and validate UTF-16 before encoding.
    private static byte[] CanonicalString(string value)
    {
        var text = new StringBuilder(value.Length + 2);
        text.Append('"');
        foreach (char character in value)
        {
            switch (character)
            {
                case '"': text.Append('\\').Append('"'); break;
                case '\\': text.Append('\\').Append('\\'); break;
                case '\b': text.Append('\\').Append('b'); break;
                case '\t': text.Append('\\').Append('t'); break;
                case '\n': text.Append('\\').Append('n'); break;
                case '\f': text.Append('\\').Append('f'); break;
                case '\r': text.Append('\\').Append('r'); break;
                default:
                    if (character < 32) text.Append('\\').Append('u').Append(((int)character).ToString("x4", System.Globalization.CultureInfo.InvariantCulture));
                    else text.Append(character);
                    break;
            }
        }
        text.Append('"');
        return new UTF8Encoding(false, true).GetBytes(text.ToString());
    }
    internal static string Digest(string domain, JsonElement value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(domain).Concat(Canonical(value)).ToArray())).ToLowerInvariant();
}
