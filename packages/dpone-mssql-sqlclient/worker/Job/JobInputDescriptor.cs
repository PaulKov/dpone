using System.Text.Json;
using Dpone.SqlClient.Startup;
using Dpone.SqlClient.Input;

namespace Dpone.SqlClient.Job;

/// <summary>Original Linux fstat values, received without performing any file observation.</summary>
public sealed record JobFileIdentity(ulong Device, ulong Inode, long Size, long MtimeNs, long CtimeNs);

/// <summary>Immutable path-free native input declaration; parsing is not EOF or fstat evidence.</summary>
public sealed class JobInputDescriptor
{
    private readonly JsonElement wire;
    /// <summary>Original inherited input descriptor number.</summary>
    public int Fd { get; }
    /// <summary>Exact physical metadata and expected complete counts, without consumption authority.</summary>
    public NativeInputContract Native { get; }
    /// <summary>Original file identity requiring independent adapter checks.</summary>
    public JobFileIdentity FileIdentity { get; }
    /// <summary>Domain-separated semantic descriptor binding.</summary>
    public string Digest => JsonWire.Digest("dpone.sqlclient.input.v1\0", wire);
    internal JsonElement Wire => wire;
    internal JobInputDescriptor(JsonElement v)
    {
        JsonWire.Shape(v, "schema_version", "fd", "columns", "expected", "max_row_bytes", "file_identity");
        JsonWire.Integer(v, "schema_version", 1, 1);
        Fd = (int)JsonWire.Integer(v, "fd", 3, int.MaxValue);
        var columns = v.GetProperty("columns");
        if (columns.ValueKind != JsonValueKind.Array || columns.GetArrayLength() is < 1 or > 100) throw JobScalars.Invalid();
        var admitted = columns.EnumerateArray().Select(Column).ToArray();
        var expected = v.GetProperty("expected"); JsonWire.Shape(expected, "rows", "encoded_bytes", "file_sha256");
        var receipt = new ExpectedInput(JsonWire.Integer(expected, "rows"), JsonWire.Integer(expected, "encoded_bytes"), JsonWire.Hash(expected, "file_sha256"));
        var file = v.GetProperty("file_identity"); JsonWire.Shape(file, "device", "inode", "size", "mtime_ns", "ctime_ns");
        FileIdentity = new(JobScalars.Unsigned(file, "device"), JobScalars.Unsigned(file, "inode"), JsonWire.Integer(file, "size"),
            JsonWire.Integer(file, "mtime_ns"), JsonWire.Integer(file, "ctime_ns"));
        if (receipt.Bytes != FileIdentity.Size || (receipt.Rows == 0) != (receipt.Bytes == 0) ||
            (receipt.Rows == 0 && receipt.Sha256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")) throw JobScalars.Invalid();
        Native = new(admitted, receipt, new((int)JsonWire.Integer(v, "max_row_bytes", 1, int.MaxValue)));
        wire = v.Clone();
    }
    private static NativeColumn Column(JsonElement v)
    {
        JsonWire.Shape(v, "name", "source_type", "target_type", "storage_type", "nullable", "prefix_width", "fixed_length", "precision", "scale", "encoding");
        string name = JobScalars.Text(v, "name", 128, true);
        string source = JobScalars.Text(v, "source_type"), target = JobScalars.Text(v, "target_type"), storage = JobScalars.Text(v, "storage_type");
        bool nullable = JobScalars.Boolean(v, "nullable");
        var column = NativeColumn.Admit(name, source, nullable, storage, (int)JsonWire.Integer(v, "prefix_width", 0, 8),
            JobScalars.OptionalInteger(v, "fixed_length"), JobScalars.OptionalInteger(v, "precision"), JobScalars.OptionalInteger(v, "scale"), JobScalars.OptionalText(v, "encoding"));
        string required = column.DataTypeName switch { "bigint" => "Int64", "float(53)" => "Float64", "nvarchar(max)" => "String", "datetime2(6)" => "DateTime64(6)", _ => throw JobScalars.Invalid() };
        if (target != (nullable ? $"Nullable({required})" : required)) throw JobScalars.Invalid();
        return column;
    }
    /// <summary>Canonical nonsecret descriptor bytes; never represent independently observed evidence.</summary>
    public byte[] Encode() => JsonWire.Canonical(wire);
    /// <summary>Closed bounded standalone descriptor parser with redacted errors.</summary>
    public static JobInputDescriptor Parse(byte[] body)
    {
        try
        {
            if (body is null || body.Length is < 1 or > SqlClientJob.MaxJobBytes) throw JobScalars.Invalid();
            using var document = JsonWire.Document(body);
            return new(document.RootElement);
        }
        catch (Exception) { throw new InvalidDataException("mssql_native.sqlclient_input_descriptor_invalid"); }
    }
}
