using System.Collections;
using System.Data;
using System.Data.Common;

namespace Dpone.SqlClient.Input;

/// <summary>Streaming exact typed DbDataReader; buffers at most the current or lookahead row.</summary>
public sealed class NativeInputReader : DbDataReader
{
    private readonly NativeInputContract contract;
    private readonly NativeRowDecoder decoder;
    private readonly CancellationToken cancellation;
    private DecodedRowLease? current, pending;
    private bool closed, seenRows, ended, failed;

    /// <summary>Create the one-shot consumer without reading or taking stream ownership.</summary>
    public NativeInputReader(Stream stream, NativeInputContract contract, CancellationToken cancellation = default)
        : this(stream, contract, cancellation, null) { }

    /// <summary>Create a reader sharing the production input-buffer ledger.</summary>
    public NativeInputReader(Stream stream, NativeInputContract contract, CancellationToken cancellation, InputBufferBudget? budget)
    {
        this.contract = contract ?? throw new ArgumentNullException(nameof(contract));
        this.cancellation = cancellation;
        decoder = new NativeRowDecoder(stream, contract, cancellation, budget);
    }

    /// <summary>Return input completeness only, independent of any bulk-copy return value.</summary>
    public InputReceipt RequireComplete() => decoder.RequireComplete();
    /// <inheritdoc/>
    public override int FieldCount => contract.Columns.Count;
    /// <inheritdoc/>
    public override bool IsClosed => closed;
    /// <inheritdoc/>
    public override int Depth => 0;
    /// <inheritdoc/>
    public override int RecordsAffected => -1;
    /// <inheritdoc/>
    public override bool HasRows
    {
        get
        {
            RequireOpen();
            try
            {
                cancellation.ThrowIfCancellationRequested();
                if (seenRows) return true;
                if (!ended && pending is null)
                {
                    pending = decoder.ReadNextLease();
                    ended = pending is null;
                    seenRows = pending is not null;
                }
                return seenRows;
            }
            catch { Fail(); throw; }
        }
    }
    /// <inheritdoc/>
    public override object this[int ordinal] => GetValue(ordinal);
    /// <inheritdoc/>
    public override object this[string name] => GetValue(GetOrdinal(name));
    /// <inheritdoc/>
    public override bool Read() => ReadCore(default);
    /// <inheritdoc/>
    public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(ReadCore(cancellationToken));

    private bool ReadCore(CancellationToken token)
    {
        RequireOpen();
        try
        {
            cancellation.ThrowIfCancellationRequested();
            token.ThrowIfCancellationRequested();
            current?.Dispose();
            current = null;
            if (ended) return false;
            current = pending ?? decoder.ReadNextLease(token);
            pending = null;
            ended = current is null;
            seenRows |= current is not null;
            return !ended;
        }
        catch
        {
            Fail();
            throw;
        }
    }
    /// <inheritdoc/>
    public override bool NextResult() { RequireOpen(); return false; }
    /// <inheritdoc/>
    public override string GetName(int ordinal) => Column(ordinal).Name;
    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal) => Column(ordinal).DataTypeName;
    /// <inheritdoc/>
    public override Type GetFieldType(int ordinal) => Column(ordinal).FieldType;
    /// <inheritdoc/>
    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < FieldCount; i++) if (contract.Columns[i].Name == name) return i;
        throw new IndexOutOfRangeException("input.column_not_found");
    }
    /// <inheritdoc/>
    public override object GetValue(int ordinal)
    {
        RequireOpen();
        Column(ordinal);
        if (current is null) throw new InvalidOperationException("input.no_current_row");
        return current.Values[ordinal];
    }
    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++) values[i] = GetValue(i);
        return count;
    }
    /// <inheritdoc/>
    public override bool IsDBNull(int ordinal) => GetValue(ordinal) is DBNull;
    /// <inheritdoc/>
    public override T GetFieldValue<T>(int ordinal) => (T)GetValue(ordinal);
    /// <inheritdoc/>
    public override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);
    /// <inheritdoc/>
    public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);
    /// <inheritdoc/>
    public override string GetString(int ordinal) => GetFieldValue<string>(ordinal);
    /// <inheritdoc/>
    public override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);
    /// <inheritdoc/>
    public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);
    /// <inheritdoc/>
    public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);
    /// <inheritdoc/>
    public override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);
    /// <inheritdoc/>
    public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);
    /// <inheritdoc/>
    public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);
    /// <inheritdoc/>
    public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);
    /// <inheritdoc/>
    public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);
    /// <inheritdoc/>
    public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);
    /// <inheritdoc/>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("input.no_binary_layout");
    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        string value = GetString(ordinal);
        if (buffer is null) return value.Length;
        if (dataOffset < 0 || dataOffset > value.Length || bufferOffset < 0 || length < 0 || bufferOffset > buffer.Length - length)
            throw new ArgumentOutOfRangeException(nameof(dataOffset), "input.character_range");
        int count = Math.Min(length, value.Length - (int)dataOffset);
        value.CopyTo((int)dataOffset, buffer, bufferOffset, count);
        return count;
    }
    /// <inheritdoc/>
    public override DataTable GetSchemaTable()
    {
        var table = new DataTable("NativeInputSchema");
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("ProviderType", typeof(int));
        table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));
        table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        for (int i = 0; i < FieldCount; i++)
        {
            NativeColumn c = Column(i);
            SqlDbType type = c.DataTypeName switch { "bigint" => SqlDbType.BigInt, "float(53)" => SqlDbType.Float,
                "nvarchar(max)" => SqlDbType.NVarChar, _ => SqlDbType.DateTime2 };
            table.Rows.Add(c.Name, i, c.FixedLength ?? int.MaxValue, c.FieldType, c.Nullable, (int)type,
                c.FixedLength is null, true, false, false);
        }
        return table;
    }
    /// <inheritdoc/>
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, false);
    /// <inheritdoc/>
    public override void Close()
    {
        if (closed) return;
        closed = true;
        ReleaseRows();
        decoder.Dispose();
    }
    /// <inheritdoc/>
    protected override void Dispose(bool disposing) { if (disposing) Close(); base.Dispose(disposing); }

    private void ReleaseRows()
    {
        current?.Dispose();
        pending?.Dispose();
        current = pending = null;
    }
    private void Fail()
    {
        failed = true;
        ReleaseRows();
        decoder.Dispose();
    }

    private NativeColumn Column(int ordinal)
    {
        if ((uint)ordinal >= (uint)FieldCount) throw new IndexOutOfRangeException("input.ordinal");
        return contract.Columns[ordinal];
    }
    private void RequireOpen()
    {
        if (closed) throw new InvalidOperationException("input.closed_reader");
        if (failed) throw new InvalidDataException("input.failed_reader");
    }
}
