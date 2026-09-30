using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Data;
using System.Security.Cryptography;
using System.Text;

namespace Dpone.Mssql.SqlClient;

internal sealed class NativeFileDataReader : IDataReader
{
    private static readonly UnicodeEncoding StrictUtf16 = new(false, false, true);
    private readonly FileStream stream;
    private readonly WriteRequest request;
    private readonly object?[] current;
    private byte[]? textBuffer;
    private IncrementalHash? rowHash;
    private byte[]? currentHash;
    private bool closed;

    internal NativeFileDataReader(WriteRequest request)
    {
        this.request = request;
        stream = new FileStream(request.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024,
            FileOptions.SequentialScan);
        current = new object?[request.Columns.Count];
    }

    internal long RowsRead { get; private set; }
    public int FieldCount => request.Columns.Count + (request.LayoutVersion == 2 ? 1 : 0);
    public bool IsClosed => closed;
    public int RecordsAffected => -1;
    public int Depth => 0;
    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));

    public bool Read()
    {
        EnsureOpen();
        if (RowsRead == request.ExpectedRows)
        {
            if (stream.Position != stream.Length)
                throw new InvalidDataException();
            return false;
        }
        if (stream.Position == stream.Length)
            return false;
        long start = stream.Position;
        if (currentHash is not null)
        {
            CryptographicOperations.ZeroMemory(currentHash);
            currentHash = null;
        }
        rowHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        for (int index = 0; index < current.Length; index++)
            current[index] = ReadValue(request.Columns[index]);
        currentHash = rowHash.GetHashAndReset();
        rowHash.Dispose();
        rowHash = null;
        if (stream.Position - start > request.MaxRowBytes)
            throw new InvalidDataException();
        RowsRead++;
        return true;
    }

    private object? ReadValue(ColumnSpec column)
    {
        return column.TargetType switch
        {
            "bigint" => DecodeInt64(column),
            "float(53)" => DecodeDouble(column),
            "datetime2(6)" => DecodeDateTime2(column),
            "nvarchar(max)" => DecodeText(column),
            _ => throw new InvalidDataException(),
        };
    }

    private object? DecodeInt64(ColumnSpec column)
    {
        if (ReadFixedLength(column, sizeof(long)) is null) return null;
        Span<byte> payload = stackalloc byte[sizeof(long)];
        ReadExactly(payload);
        return BinaryPrimitives.ReadInt64LittleEndian(payload);
    }

    private object? DecodeDouble(ColumnSpec column)
    {
        if (ReadFixedLength(column, sizeof(long)) is null) return null;
        Span<byte> payload = stackalloc byte[sizeof(long)];
        ReadExactly(payload);
        return Finite(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(payload)));
    }

    private object? DecodeDateTime2(ColumnSpec column)
    {
        if (ReadFixedLength(column, 8) is null) return null;
        Span<byte> payload = stackalloc byte[8];
        ReadExactly(payload);
        return DecodeDateTime2(payload);
    }

    private int? ReadFixedLength(ColumnSpec column, int width)
    {
        int length = column.Nullable ? ReadLength() : width;
        if (length == -1) return column.Nullable ? null : throw new InvalidDataException();
        return length == width ? length : throw new InvalidDataException();
    }

    private object? DecodeText(ColumnSpec column)
    {
        long length = ReadLength64();
        if (length == -1)
            return column.Nullable ? null : throw new InvalidDataException();
        if (length < 0 || length > request.MaxRowBytes || length % 2 != 0 || length > int.MaxValue)
            throw new InvalidDataException();
        int count = checked((int)length);
        if (count == 0) return string.Empty;
        byte[] rented = RequireTextBuffer(count);
        try
        {
            Span<byte> payload = rented.AsSpan(0, count);
            ReadExactly(payload);
            return StrictUtf16.GetString(payload);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(rented.AsSpan(0, count));
        }
    }

    private byte[] RequireTextBuffer(int count)
    {
        if (textBuffer is not null && textBuffer.Length >= count) return textBuffer;
        if (textBuffer is not null)
        {
            CryptographicOperations.ZeroMemory(textBuffer);
            ArrayPool<byte>.Shared.Return(textBuffer);
        }
        textBuffer = ArrayPool<byte>.Shared.Rent(count);
        return textBuffer;
    }

    private int ReadLength()
    {
        Span<byte> value = stackalloc byte[1];
        ReadExactly(value);
        return unchecked((sbyte)value[0]);
    }

    private long ReadLength64()
    {
        Span<byte> value = stackalloc byte[sizeof(long)];
        ReadExactly(value);
        return BinaryPrimitives.ReadInt64LittleEndian(value);
    }

    private void ReadExactly(Span<byte> destination)
    {
        if (destination.Length > request.MaxRowBytes || destination.Length > stream.Length - stream.Position)
            throw new InvalidDataException();
        stream.ReadExactly(destination);
        rowHash?.AppendData(destination);
    }

    private static object Finite(double value) => double.IsFinite(value) ? value : throw new InvalidDataException();

    private static object DecodeDateTime2(ReadOnlySpan<byte> payload)
    {
        long timeTicks = 0;
        for (int index = 4; index >= 0; index--)
            timeTicks = checked(timeTicks * 256 + payload[index]);
        int days = payload[5] | payload[6] << 8 | payload[7] << 16;
        if (timeTicks >= TimeSpan.TicksPerDay || timeTicks % 10 != 0 || days > 3_652_058)
            throw new InvalidDataException();
        return new DateTime(checked(days * TimeSpan.TicksPerDay + timeTicks), DateTimeKind.Unspecified);
    }

    public string GetName(int i) => i == request.Columns.Count && request.LayoutVersion == 2
        ? "__dpone__native_row_hash" : request.Columns[i].TargetName;
    public string GetDataTypeName(int i) => i == request.Columns.Count && request.LayoutVersion == 2
        ? "binary(32)" : request.Columns[i].TargetType;
    public Type GetFieldType(int i) => i == request.Columns.Count && request.LayoutVersion == 2
        ? typeof(byte[]) : request.Columns[i].TargetType switch
    {
        "bigint" => typeof(long),
        "float(53)" => typeof(double),
        "nvarchar(max)" => typeof(string),
        "datetime2(6)" => typeof(DateTime),
        _ => throw new InvalidDataException(),
    };
    public object GetValue(int i) => i == request.Columns.Count && request.LayoutVersion == 2
        ? currentHash ?? throw new InvalidDataException() : current[i] ?? DBNull.Value;
    public int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int index = 0; index < count; index++) values[index] = GetValue(index);
        return count;
    }
    public int GetOrdinal(string name)
    {
        for (int index = 0; index < request.Columns.Count; index++)
            if (string.Equals(request.Columns[index].TargetName, name, StringComparison.OrdinalIgnoreCase)) return index;
        if (request.LayoutVersion == 2 &&
            string.Equals(name, "__dpone__native_row_hash", StringComparison.OrdinalIgnoreCase))
            return request.Columns.Count;
        throw new IndexOutOfRangeException();
    }
    public bool GetBoolean(int i) => (bool)GetValue(i);
    public byte GetByte(int i) => (byte)GetValue(i);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length) =>
        Copy((byte[])GetValue(i), fieldOffset, buffer, bufferOffset, length);
    public char GetChar(int i) => (char)GetValue(i);
    public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length) =>
        Copy(((string)GetValue(i)).ToCharArray(), fieldOffset, buffer, bufferOffset, length);
    public Guid GetGuid(int i) => (Guid)GetValue(i);
    public short GetInt16(int i) => Convert.ToInt16(GetValue(i));
    public int GetInt32(int i) => Convert.ToInt32(GetValue(i));
    public long GetInt64(int i) => Convert.ToInt64(GetValue(i));
    public float GetFloat(int i) => Convert.ToSingle(GetValue(i));
    public double GetDouble(int i) => Convert.ToDouble(GetValue(i));
    public string GetString(int i) => (string)GetValue(i);
    public decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i));
    public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
    public IDataReader GetData(int i) => throw new NotSupportedException();
    public bool IsDBNull(int i) => i == request.Columns.Count && request.LayoutVersion == 2
        ? currentHash is null : current[i] is null;
    public DataTable? GetSchemaTable() => null;
    public bool NextResult() => false;

    private static long Copy<T>(T[] source, long fieldOffset, T[]? destination, int destinationOffset, int length)
    {
        if (destination is null) return source.Length;
        int count = Math.Min(length, source.Length - checked((int)fieldOffset));
        Array.Copy(source, checked((int)fieldOffset), destination, destinationOffset, count);
        return count;
    }

    private void EnsureOpen()
    {
        if (closed) throw new ObjectDisposedException(nameof(NativeFileDataReader));
    }
    public void Close() => Dispose();
    public void Dispose()
    {
        if (closed) return;
        closed = true;
        Array.Clear(current);
        if (currentHash is not null) CryptographicOperations.ZeroMemory(currentHash);
        rowHash?.Dispose();
        if (textBuffer is not null)
        {
            CryptographicOperations.ZeroMemory(textBuffer);
            ArrayPool<byte>.Shared.Return(textBuffer);
            textBuffer = null;
        }
        stream.Dispose();
    }
}
