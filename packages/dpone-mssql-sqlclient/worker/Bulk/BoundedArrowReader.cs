using System.Text;
using System.Collections;
using System.Data.Common;
using Apache.Arrow;
using Dpone.SqlClient.Input;

namespace Dpone.SqlClient.Bulk;

/// <summary>
/// Exact-size Arrow batches over the same native decoder. The shared budget
/// charges native leases, simultaneous Arrow buffers and SDK scalar conversion.
/// Arrow and input completeness alone do not certify SQL delivery or settlement.
/// </summary>
public sealed class BoundedArrowReader : DbDataReader
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly InputBufferBudget budget;
    private readonly NativeInputContract Contract;
    private readonly NativeRowDecoder decoder;
    private readonly ArrowBatchAssembler assembler;
    private readonly CancellationToken cancellation;
    private LeasedArrowBatch? batch;
    private InputBufferReservation? valuesReservation;
    private object[]? values;
    private int row = -1;
    private bool closed, failed, ended, seenRows;

    /// <summary>Bind caller-owned input and one exact shared decoded-buffer budget.</summary>
    public BoundedArrowReader(Stream stream, NativeInputContract contract, BulkInputPolicy policy,
        InputBufferBudget budget, CancellationToken cancellation = default)
    {
        if (budget.LimitBytes != policy.MaxInputBatchBytes) throw new InvalidDataException("bulk.budget_identity");
        Contract = contract;
        this.budget = budget;
        this.cancellation = cancellation;
        decoder = new NativeRowDecoder(stream, contract, cancellation, budget);
        assembler = new ArrowBatchAssembler(decoder, contract, policy, budget);
    }
    /// <summary>Return original native EOF authority only after the SDK reader also reached EOF.</summary>
    public InputReceipt RequireComplete()
    {
        if (!ended || failed) throw new InvalidDataException("bulk.incomplete_input");
        return decoder.RequireComplete();
    }
    /// <summary>Observed maximum batch rows; no payload values are recorded.</summary>
    public int MaxObservedBatchRows { get; private set; }
    /// <inheritdoc/>
    public override bool IsClosed => closed;
    /// <inheritdoc/>
    public override bool HasRows
    {
        get
        {
            RequireOpen();
            if (seenRows) return true;
            if (!ended && batch is null) LoadBatch(cancellation);
            return seenRows;
        }
    }
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
            ReleaseValues();
            if (ended) return false;
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, token);
            if (batch is null || row + 1 >= batch.Batch.Length)
            {
                batch?.Dispose();
                batch = null;
                LoadBatch(linked.Token);
                if (ended) return false;
            }
            row++;
            MaterializeValues(linked.Token);
            return true;
        }
        catch { Fail(); throw; }
    }

    private void LoadBatch(CancellationToken token)
    {
        try
        {
            cancellation.ThrowIfCancellationRequested();
            batch = assembler.Next(token);
            row = -1;
            ended = batch is null;
            seenRows |= !ended;
            if (batch is not null) MaxObservedBatchRows = Math.Max(MaxObservedBatchRows, batch.Batch.Length);
        }
        catch { Fail(); throw; }
    }

    private void MaterializeValues(CancellationToken token)
    {
        long retained = checked(8L * FieldCount);
        for (int i = 0; i < FieldCount; i++)
        {
            token.ThrowIfCancellationRequested();
            var array = batch!.Batch.Column(i);
            if (!array.IsNull(row)) retained = checked(retained + (array is StringArray text ? 2L * Utf8.GetCharCount(text.GetBytes(row)) : 8));
        }
        valuesReservation = budget.Reserve(retained);
        values = new object[FieldCount];
        for (int i = 0; i < FieldCount; i++)
        {
            token.ThrowIfCancellationRequested();
            var array = batch!.Batch.Column(i);
            values[i] = array.IsNull(row) ? DBNull.Value : array switch {
                Int64Array number => number.GetValue(row)!.Value,
                DoubleArray number => number.GetValue(row)!.Value,
                StringArray text => text.GetString(row, Utf8),
                TimestampArray time => new DateTime(checked(DateTime.UnixEpoch.Ticks + time.GetValue(row)!.Value * 10), DateTimeKind.Unspecified),
                _ => throw new InvalidDataException("bulk.arrow_type") };
        }
    }
    /// <inheritdoc/>
    public override object GetValue(int ordinal)
    {
        RequireOpen();
        if ((uint)ordinal >= (uint)FieldCount) throw new IndexOutOfRangeException("bulk.ordinal");
        if (values is null) throw new InvalidOperationException("bulk.no_current_row");
        return values[ordinal];
    }
    private void ReleaseValues()
    {
        values = null;
        valuesReservation?.Dispose();
        valuesReservation = null;
    }
    private void Fail()
    {
        failed = true;
        ReleaseValues();
        batch?.Dispose();
        batch = null;
        assembler.Dispose();
    }
    private void RequireOpen()
    {
        if (closed || failed) throw new InvalidOperationException("bulk.reader_unavailable");
    }
    /// <inheritdoc/>
    public override void Close()
    {
        if (closed) return;
        closed = true;
        ReleaseValues();
        batch?.Dispose();
        batch = null;
        assembler.Dispose();
    }
    /// <inheritdoc/>
    protected override void Dispose(bool disposing) { if (disposing) Close(); base.Dispose(disposing); }
    /// <inheritdoc/>
    public override int FieldCount => Contract.Columns.Count;
    /// <inheritdoc/>
    public override int Depth => 0;
    /// <inheritdoc/>
    public override int RecordsAffected => -1;
    /// <inheritdoc/>
    public override object this[int ordinal] => GetValue(ordinal);
    /// <inheritdoc/>
    public override object this[string name] => GetValue(GetOrdinal(name));
    /// <inheritdoc/>
    public override bool NextResult() => false;
    /// <inheritdoc/>
    public override string GetName(int ordinal) => Contract.Columns[ordinal].Name;
    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal) => Contract.Columns[ordinal].DataTypeName;
    /// <inheritdoc/>
    public override Type GetFieldType(int ordinal) => Contract.Columns[ordinal].FieldType;
    /// <inheritdoc/>
    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < FieldCount; i++) if (GetName(i) == name) return i;
        throw new IndexOutOfRangeException("bulk.column_not_found");
    }
    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        int count = Math.Min(FieldCount, values.Length);
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
    public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);
    /// <inheritdoc/>
    public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);
    /// <inheritdoc/>
    public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);
    /// <inheritdoc/>
    public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);
    /// <inheritdoc/>
    public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);
    /// <inheritdoc/>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("bulk.no_binary_layout");
    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        string value = GetString(ordinal);
        if (buffer is null) return value.Length;
        if (dataOffset < 0 || dataOffset > value.Length || bufferOffset < 0 || length < 0 || bufferOffset > buffer.Length - length)
            throw new ArgumentOutOfRangeException(nameof(dataOffset), "bulk.character_range");
        int count = Math.Min(length, value.Length - (int)dataOffset);
        value.CopyTo((int)dataOffset, buffer, bufferOffset, count);
        return count;
    }
    /// <inheritdoc/>
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, false);
}
