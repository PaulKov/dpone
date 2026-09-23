using System.Collections.ObjectModel;

namespace Dpone.SqlClient.Input;

/// <summary>Immutable admitted physical column in the existing native format.</summary>
public sealed class NativeColumn
{
    /// <summary>Exact ordered source name; never interpolated into SQL by this component.</summary>
    public string Name { get; }
    /// <summary>Declared SQL type, without the nullability suffix.</summary>
    public string DataTypeName { get; }
    /// <summary>Whether the native null indicator is permitted.</summary>
    public bool Nullable { get; }
    /// <summary>Physical signed little-endian prefix width.</summary>
    public int PrefixWidth { get; }
    /// <summary>Fixed physical payload length, or null for variable Unicode text.</summary>
    public int? FixedLength { get; }
    /// <summary>Exact managed scalar type exposed to the consumer.</summary>
    public Type FieldType { get; }

    private NativeColumn(string name, string type, bool nullable, int prefix, int? length, Type fieldType)
        => (Name, DataTypeName, Nullable, PrefixWidth, FixedLength, FieldType) = (name, type, nullable, prefix, length, fieldType);

    /// <summary>Reject inconsistent metadata before reading any input stream.</summary>
    public static NativeColumn Admit(string name, string sourceType, bool nullable, string storageType,
        int prefixWidth, int? fixedLength, int? precision, int? scale, string? encoding)
    {
        if (string.IsNullOrEmpty(name) || name.Length > 128 || name.IndexOf('\0') >= 0)
            throw new InvalidDataException("input.column_name");
        if (sourceType is null) throw new InvalidDataException("input.unsupported_layout");
        string declared = nullable && sourceType.EndsWith(" nullable", StringComparison.Ordinal) ? sourceType[..^9] : sourceType;
        if ((nullable && declared == sourceType) || (!nullable && sourceType.EndsWith(" nullable", StringComparison.Ordinal)))
            throw new InvalidDataException("input.nullability");
        var expected = declared switch
        {
            "bigint" => ("bigint", (int?)8, (int?)null, (int?)null, (string?)null, typeof(long)),
            "float(53)" => ("float", (int?)8, (int?)53, (int?)null, (string?)null, typeof(double)),
            "nvarchar(max)" => ("nvarchar", (int?)null, (int?)null, (int?)null, "utf-16le", typeof(string)),
            "datetime2(6)" => ("datetime2", (int?)8, (int?)null, (int?)7, (string?)null, typeof(DateTime)),
            _ => throw new InvalidDataException("input.unsupported_layout")
        };
        int prefix = declared == "nvarchar(max)" ? 8 : nullable ? 1 : 0;
        if (storageType != expected.Item1 || fixedLength != expected.Item2 || precision != expected.Item3 ||
            scale != expected.Item4 || encoding != expected.Item5 || prefixWidth != prefix)
            throw new InvalidDataException("input.inconsistent_layout");
        return new NativeColumn(name, declared, nullable, prefix, fixedLength, expected.Item6);
    }
}

/// <summary>Authenticated expected input supplied by the upstream sealed-handle owner.</summary>
public sealed class ExpectedInput
{
    /// <summary>Expected complete row count; always a 64-bit value.</summary>
    public long Rows { get; }
    /// <summary>Expected encoded byte count; always a 64-bit value.</summary>
    public long Bytes { get; }
    /// <summary>Canonical lowercase SHA256 of the exact encoded stream.</summary>
    public string Sha256 { get; }
    /// <summary>Validate closed count/hash fields without opening a file.</summary>
    public ExpectedInput(long rows, long bytes, string sha256)
    {
        if (rows < 0 || bytes < 0 || sha256 is null || sha256.Length != 64 ||
            sha256.Any(c => !(c is >= '0' and <= '9' or >= 'a' and <= 'f')))
            throw new InvalidDataException("input.expected_identity");
        (Rows, Bytes, Sha256) = (rows, bytes, sha256);
    }
}

/// <summary>Encoded allocation bound; decoded batching and process limits belong upstream.</summary>
public sealed class NativeInputLimits
{
    /// <summary>Maximum encoded bytes in any row, including native prefixes.</summary>
    public int MaxRowBytes { get; }
    /// <summary>Admit a positive finite per-row allocation bound.</summary>
    public NativeInputLimits(int maxRowBytes)
    {
        if (maxRowBytes <= 0) throw new InvalidDataException("input.row_limit");
        MaxRowBytes = maxRowBytes;
    }
}

/// <summary>Closed snapshot of admitted metadata and authenticated input expectations.</summary>
public sealed class NativeInputContract
{
    /// <summary>Immutable columns in exact source order.</summary>
    public ReadOnlyCollection<NativeColumn> Columns { get; }
    /// <summary>Expected complete input authority; not evidence of SQL delivery.</summary>
    public ExpectedInput Expected { get; }
    /// <summary>Finite native allocation bound.</summary>
    public NativeInputLimits Limits { get; }
    /// <summary>Copy the ordered collection so caller mutations cannot change admission.</summary>
    public NativeInputContract(IEnumerable<NativeColumn> columns, ExpectedInput expected, NativeInputLimits limits)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(limits);
        var copy = columns.Take(101).ToArray();
        if (copy.Length is < 1 or > 100 || copy.Any(c => c is null) ||
            copy.Select(c => c.Name).Distinct(StringComparer.Ordinal).Count() != copy.Length)
            throw new InvalidDataException("input.columns");
        Columns = Array.AsReadOnly(copy);
        Expected = expected;
        Limits = limits;
    }
}

/// <summary>Natural EOF receipt for consumed native bytes only; never SQL delivery proof.</summary>
public sealed record InputReceipt(long Rows, long Bytes, string Sha256);
