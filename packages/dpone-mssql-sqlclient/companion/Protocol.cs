using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Dpone.Mssql.SqlClient;

internal sealed record ColumnSpec(int Ordinal, string TargetName, string TargetType, bool Nullable);

internal sealed record WriteRequest(
    string Protocol,
    int LayoutVersion,
    string AttemptId,
    string QualifiedStage,
    string StageIdSha256,
    string OwnerBindingSha256,
    int ObjectId,
    string SchemaSha256,
    string FilePath,
    long ExpectedRows,
    long EncodedBytes,
    int MaxRowBytes,
    string FileSha256,
    string GrantTokenSha256,
    string ProofCapability,
    string WireLayoutSha256,
    int DeadlineBudgetMs,
    IReadOnlyList<ColumnSpec> Columns);

internal sealed record SqlCredentials(
    string Host,
    int Port,
    string Database,
    string Username,
    string Password,
    bool Encrypt,
    bool TrustServerCertificate);

internal sealed class CompanionProtocolException(string code) : Exception
{
    internal string Code { get; } = code;
}

internal sealed class ParsedFrame(byte[] payload, JsonDocument document) : IDisposable
{
    internal JsonElement Root => document.RootElement;

    public void Dispose()
    {
        document.Dispose();
        CryptographicOperations.ZeroMemory(payload);
    }
}

internal static partial class Protocol
{
    internal const string Id = "dpone.mssql-sqlclient.ipc.v1";
    internal const int RequestMaxBytes = 1024 * 1024;
    internal const int CredentialMaxBytes = 64 * 1024;
    private static readonly Regex Sha256Pattern = Sha256Regex();
    private static readonly Regex StagePattern = StageRegex();

    internal static WriteRequest ReadRequest(Stream stream)
    {
        ParsedFrame parsed;
        try
        {
            parsed = ReadCanonicalFrame(stream, RequestMaxBytes);
        }
        catch (Exception exception) when (exception is not CompanionProtocolException)
        {
            throw new CompanionProtocolException("mssql_sqlclient.invalid_request_frame");
        }
        using ParsedFrame frame = parsed;
        try
        {
            JsonElement root = frame.Root;
            int schemaVersion;
            string protocol;
            try
            {
                schemaVersion = Integer(root, "schema_version");
                protocol = Text(root, "protocol");
                string[] common = [
                    "schema_version", "protocol", "attempt_id", "qualified_stage", "stage_id_sha256",
                    "owner_binding_sha256", "object_id", "schema_sha256", "file_path", "expected_rows",
                    "encoded_bytes", "max_row_bytes", "file_sha256", "grant_token_sha256", "proof_capability",
                    "wire_layout_sha256", "deadline_budget_ms", "columns"];
                RequireFields(root, schemaVersion == 2 ? [.. common, "layout_version"] : common);
            }
            catch (Exception)
            {
                throw new CompanionProtocolException("mssql_sqlclient.invalid_request_fields");
            }
            if (schemaVersion is not (1 or 2) || protocol != ProtocolId(schemaVersion) ||
                (schemaVersion == 2 && Integer(root, "layout_version") != 2))
                throw new CompanionProtocolException("mssql_sqlclient.invalid_request_version");
            List<ColumnSpec> columns = [];
            JsonElement rawColumns = root.GetProperty("columns");
            if (rawColumns.ValueKind != JsonValueKind.Array)
                throw new CompanionProtocolException("mssql_sqlclient.invalid_columns_shape");
            foreach (JsonElement raw in rawColumns.EnumerateArray())
            {
                try
                {
                    RequireFields(raw, "ordinal", "target_name", "target_type", "nullable");
                    columns.Add(new ColumnSpec(
                        Integer(raw, "ordinal"), Text(raw, "target_name"), Text(raw, "target_type"), Boolean(raw, "nullable")));
                }
                catch (Exception)
                {
                    throw new CompanionProtocolException("mssql_sqlclient.invalid_columns_payload");
                }
            }
            WriteRequest request;
            try
            {
                request = new(
                    protocol, schemaVersion, Text(root, "attempt_id"), Text(root, "qualified_stage"),
                    Digest(root, "stage_id_sha256"),
                    Digest(root, "owner_binding_sha256"), Integer(root, "object_id"), Digest(root, "schema_sha256"),
                    Text(root, "file_path"), LongInteger(root, "expected_rows"), LongInteger(root, "encoded_bytes"),
                    Integer(root, "max_row_bytes"), Digest(root, "file_sha256"), Digest(root, "grant_token_sha256"),
                    Text(root, "proof_capability"), Digest(root, "wire_layout_sha256"), Integer(root, "deadline_budget_ms"),
                    columns);
            }
            catch (Exception)
            {
                throw new CompanionProtocolException("mssql_sqlclient.invalid_request_payload");
            }
            Validate(request);
            return request;
        }
        catch (Exception exception) when (exception is not CompanionProtocolException)
        {
            throw new CompanionProtocolException("mssql_sqlclient.invalid_request_contract");
        }
    }

    internal static SqlCredentials ReadCredentials(Stream stream)
    {
        ParsedFrame parsed;
        try
        {
            parsed = ReadCanonicalFrame(stream, CredentialMaxBytes);
        }
        catch (Exception exception) when (exception is not CompanionProtocolException)
        {
            throw new CompanionProtocolException("mssql_sqlclient.invalid_credentials_frame");
        }
        using ParsedFrame frame = parsed;
        try
        {
            JsonElement root = frame.Root;
            RequireFields(root, "schema_version", "authentication", "host", "port", "database", "username", "password",
                "encrypt", "trust_server_certificate");
            if (Text(root, "schema_version") != "dpone.mssql-sqlclient.credentials.v1" ||
                Text(root, "authentication") != "sql_password")
                throw new InvalidDataException();
            SqlCredentials credentials = new(
                Text(root, "host"), Integer(root, "port"), Text(root, "database"), Text(root, "username"),
                Text(root, "password"), Boolean(root, "encrypt"), Boolean(root, "trust_server_certificate"));
            if (!Bounded(credentials.Host, 255) || credentials.Port is < 1 or > 65535 ||
                !Bounded(credentials.Database, 128) || !Bounded(credentials.Username, 128) ||
                !Bounded(credentials.Password, 2048) || !credentials.Encrypt)
                throw new InvalidDataException();
            return credentials;
        }
        catch (Exception exception) when (exception is not CompanionProtocolException)
        {
            throw new CompanionProtocolException("mssql_sqlclient.invalid_credentials_contract");
        }
    }

    internal static void WriteResult(
        Stream stream, WriteRequest request, string classification, long? rows, string writerIdentity,
        double? launchSeconds, double? writeSeconds, double? disposeSeconds)
    {
        var result = new SortedDictionary<string, object?>(StringComparer.Ordinal)
        {
            ["attempt_id"] = request.AttemptId,
            ["classification"] = classification,
            ["input_rows_consumed"] = rows,
            ["metrics"] = new SortedDictionary<string, object?>(StringComparer.Ordinal)
            {
                ["dispose_seconds"] = disposeSeconds,
                ["launch_seconds"] = launchSeconds,
                ["write_seconds"] = writeSeconds,
            },
            ["protocol"] = request.Protocol,
            ["runtime_identity_sha256"] = RuntimeIdentity,
            ["schema_version"] = request.LayoutVersion,
            ["writer_identity_sha256"] = writerIdentity,
        };
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(result);
        Span<byte> header = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(header, checked((uint)payload.Length));
        stream.Write(header);
        stream.Write(payload);
        stream.Flush();
        CryptographicOperations.ZeroMemory(payload);
    }

    internal static string RuntimeIdentity => Convert.ToHexString(
        SHA256.HashData(Encoding.UTF8.GetBytes("Microsoft.NETCore.App\010"))).ToLowerInvariant();

    internal static string ApplicationLock(string grantTokenSha256) => "dpone:mssql-native:" + Convert.ToHexString(
        SHA256.HashData(Encoding.UTF8.GetBytes("dpone.mssql-sqlclient.applock.v1\0" + grantTokenSha256))).ToLowerInvariant();

    internal static string ApplicationName(WriteRequest request) => ApplicationName(
        request.AttemptId, request.GrantTokenSha256, request.ObjectId, request.StageIdSha256);

    internal static string ApplicationNameSelfTest() => ApplicationName(
        "dpone-parity-vector", new string('5', 64), 781, new string('6', 64));

    private static string ApplicationName(string attemptId, string grantTokenSha256, int objectId, string stageIdSha256)
    {
        string identity = string.Join("\0", [
            "dpone.mssql-sqlclient.application.v2",
            attemptId,
            grantTokenSha256,
            objectId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            stageIdSha256,
        ]);
        return "dpone-mssql-sqlclient:" + Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant();
    }

    private static string ProtocolId(int layoutVersion) => layoutVersion == 1 ? Id : "dpone.mssql-sqlclient.ipc.v2";

    private static ParsedFrame ReadCanonicalFrame(Stream stream, int limit)
    {
        Span<byte> header = stackalloc byte[4];
        stream.ReadExactly(header);
        uint size = BinaryPrimitives.ReadUInt32BigEndian(header);
        if (size is 0 || size > limit)
            throw new InvalidDataException();
        byte[] payload = GC.AllocateUninitializedArray<byte>(checked((int)size));
        try
        {
            stream.ReadExactly(payload);
            if (stream.ReadByte() != -1)
                throw new InvalidDataException();
            JsonDocument document = JsonDocument.Parse(payload, new JsonDocumentOptions { MaxDepth = 32 });
            RejectDuplicateProperties(document.RootElement);
            using MemoryStream canonical = new();
            using (Utf8JsonWriter writer = new(canonical))
                WriteCanonical(writer, document.RootElement);
            if (!payload.AsSpan().SequenceEqual(canonical.GetBuffer().AsSpan(0, checked((int)canonical.Length))))
            {
                document.Dispose();
                throw new InvalidDataException();
            }
            CryptographicOperations.ZeroMemory(canonical.GetBuffer().AsSpan(0, checked((int)canonical.Length)));
            return new ParsedFrame(payload, document);
        }
        catch
        {
            CryptographicOperations.ZeroMemory(payload);
            throw;
        }
    }

    private static void RejectDuplicateProperties(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            HashSet<string> names = new(StringComparer.Ordinal);
            foreach (JsonProperty property in element.EnumerateObject())
            {
                if (!names.Add(property.Name))
                    throw new InvalidDataException();
                RejectDuplicateProperties(property.Value);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
            foreach (JsonElement item in element.EnumerateArray())
                RejectDuplicateProperties(item);
    }

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            writer.WriteStartObject();
            foreach (JsonProperty property in element.EnumerateObject().OrderBy(item => item.Name, StringComparer.Ordinal))
            {
                writer.WritePropertyName(property.Name);
                WriteCanonical(writer, property.Value);
            }
            writer.WriteEndObject();
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            writer.WriteStartArray();
            foreach (JsonElement item in element.EnumerateArray())
                WriteCanonical(writer, item);
            writer.WriteEndArray();
        }
        else
            element.WriteTo(writer);
    }

    private static void Validate(WriteRequest request)
    {
        if (request.LayoutVersion is not (1 or 2) || request.Protocol != ProtocolId(request.LayoutVersion))
            throw new CompanionProtocolException("mssql_sqlclient.invalid_request_version");
        if (!Bounded(request.AttemptId, 256))
            throw new CompanionProtocolException("mssql_sqlclient.invalid_attempt_identity");
        if (!StagePattern.IsMatch(request.QualifiedStage))
            throw new CompanionProtocolException("mssql_sqlclient.invalid_stage_identity");
        if (request.ObjectId < 1)
            throw new CompanionProtocolException("mssql_sqlclient.invalid_object_identity");
        if (!Path.IsPathFullyQualified(request.FilePath))
            throw new CompanionProtocolException("mssql_sqlclient.invalid_file_path");
        if (request.ExpectedRows < 0 || request.EncodedBytes < 0 || request.MaxRowBytes < 1 ||
            (request.ExpectedRows > 0 && request.MaxRowBytes > request.EncodedBytes))
            throw new CompanionProtocolException("mssql_sqlclient.invalid_file_bounds");
        if (request.DeadlineBudgetMs < 1)
            throw new CompanionProtocolException("mssql_sqlclient.invalid_deadline_budget");
        if (request.ProofCapability != "sqlclient-session-applock-v1")
            throw new CompanionProtocolException("mssql_sqlclient.invalid_proof_capability");
        if (request.Columns.Count == 0)
            throw new CompanionProtocolException("mssql_sqlclient.invalid_columns");
        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);
        for (int index = 0; index < request.Columns.Count; index++)
        {
            ColumnSpec column = request.Columns[index];
            if (column.Ordinal != index || !Bounded(column.TargetName, 128) || !names.Add(column.TargetName) ||
                column.TargetType is not ("bigint" or "float(53)" or "nvarchar(max)" or "datetime2(6)"))
                throw new CompanionProtocolException("mssql_sqlclient.invalid_column_mapping");
        }
    }

    private static void RequireFields(JsonElement value, params string[] fields)
    {
        if (value.ValueKind != JsonValueKind.Object ||
            !value.EnumerateObject().Select(item => item.Name).ToHashSet(StringComparer.Ordinal).SetEquals(fields))
            throw new InvalidDataException();
    }

    private static string Text(JsonElement value, string name) => value.GetProperty(name).ValueKind == JsonValueKind.String
        ? value.GetProperty(name).GetString()! : throw new InvalidDataException();
    private static int Integer(JsonElement value, string name) => value.GetProperty(name).TryGetInt32(out int result)
        ? result : throw new InvalidDataException();
    private static long LongInteger(JsonElement value, string name) => value.GetProperty(name).TryGetInt64(out long result)
        ? result : throw new InvalidDataException();
    private static bool Boolean(JsonElement value, string name) => value.GetProperty(name).ValueKind is JsonValueKind.True or JsonValueKind.False
        ? value.GetProperty(name).GetBoolean() : throw new InvalidDataException();
    private static string Digest(JsonElement value, string name)
    {
        string result = Text(value, name);
        return Sha256Pattern.IsMatch(result) ? result : throw new InvalidDataException();
    }
    private static bool Bounded(string value, int limit) => value.Length is > 0 && value.Length <= limit &&
        value.All(character => character is >= ' ' and not '\u007f');

    [GeneratedRegex("[0-9a-f]{64}\\z", RegexOptions.CultureInvariant)]
    private static partial Regex Sha256Regex();
    [GeneratedRegex("\\[(?:[^\\]\\x00-\\x1f]|\\]\\])+\\](?:\\.\\[(?:[^\\]\\x00-\\x1f]|\\]\\])+\\]){1,2}\\z", RegexOptions.CultureInvariant)]
    private static partial Regex StageRegex();
}
