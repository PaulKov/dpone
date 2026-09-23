using System.Buffers.Binary;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Dpone.SqlClient.Input;

namespace Dpone.SqlClient.Bulk;

internal sealed class LeasedArrowBatch : IDisposable
{
    internal RecordBatch Batch { get; }
    private readonly List<InputBufferReservation> reservations;
    internal LeasedArrowBatch(RecordBatch batch, List<InputBufferReservation> reservations)
        => (Batch, this.reservations) = (batch, reservations);
    public void Dispose()
    {
        try { Batch.Dispose(); }
        finally { foreach (var reservation in reservations) reservation.Dispose(); }
    }
}

internal sealed class ArrowBatchAssembler : IDisposable
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly NativeRowDecoder decoder;
    private readonly NativeInputContract contract;
    private readonly BulkInputPolicy policy;
    private readonly InputBufferBudget budget;
    private DecodedRowLease? pending;
    private bool ended, failed;

    internal ArrowBatchAssembler(NativeRowDecoder decoder, NativeInputContract contract, BulkInputPolicy policy, InputBufferBudget budget)
        => (this.decoder, this.contract, this.policy, this.budget) = (decoder, contract, policy, budget);

    internal LeasedArrowBatch? Next(CancellationToken cancellation)
    {
        if (failed) throw new InvalidDataException("bulk.failed_input");
        var rows = new List<DecodedRowLease>();
        var reservations = new List<InputBufferReservation>();
        var arrays = new List<IArrowArray>();
        try
        {
            while (!ended && rows.Count < policy.BatchRows)
            {
                cancellation.ThrowIfCancellationRequested();
                long safeNext = checked(16L * contract.Columns.Count + 2L * contract.Limits.MaxRowBytes + 9);
                if (rows.Count > 0 && pending is null && budget.LimitBytes - budget.UsedBytes < safeNext) break;
                var row = pending ?? decoder.ReadNextLease(cancellation);
                pending = null;
                if (row is null) { ended = true; break; }
                pending = row;
                long increment = AdditionalBytes(row.Values, rows.Count);
                if (increment > budget.LimitBytes - budget.UsedBytes)
                {
                    if (rows.Count == 0) { row.Dispose(); throw new InvalidDataException("bulk.single_row_budget_exceeded"); }
                    pending = row;
                    break;
                }
                rows.Add(row);
                pending = null;
                reservations.Add(budget.Reserve(increment));
            }
            if (rows.Count == 0) return null;
            for (int i = 0; i < contract.Columns.Count; i++)
            {
                cancellation.ThrowIfCancellationRequested();
                arrays.Add(BuildColumn(i, rows, cancellation));
            }
            var fields = contract.Columns.Select(c => new Field(c.Name, ArrowType(c), c.Nullable, null));
            var result = new LeasedArrowBatch(new RecordBatch(new Schema(fields, null), arrays, rows.Count), reservations);
            arrays.Clear();
            reservations = new List<InputBufferReservation>();
            return result;
        }
        catch
        {
            failed = true;
            pending?.Dispose();
            pending = null;
            throw;
        }
        finally
        {
            foreach (var row in rows) row.Dispose();
            foreach (var array in arrays) array.Dispose();
            foreach (var reservation in reservations) reservation.Dispose();
        }
    }

    private long AdditionalBytes(object[] values, int count)
    {
        long bytes = 0;
        for (int i = 0; i < contract.Columns.Count; i++)
        {
            var column = contract.Columns[i];
            if (column.Nullable && count % 8 == 0) bytes = checked(bytes + 1);
            bytes = checked(bytes + (column.FieldType == typeof(string)
                ? 4 + (count == 0 ? 4 : 0) + (values[i] is DBNull ? 0 : Utf8.GetByteCount((string)values[i]))
                : 8));
        }
        return bytes;
    }

    private IArrowArray BuildColumn(int ordinal, List<DecodedRowLease> rows, CancellationToken cancellation)
    {
        NativeColumn column = contract.Columns[ordinal];
        int count = rows.Count;
        var validity = column.Nullable ? new byte[(count + 7) / 8] : System.Array.Empty<byte>();
        int nullCount = 0;
        for (int i = 0; i < count; i++)
        {
            cancellation.ThrowIfCancellationRequested();
            if (rows[i].Values[ordinal] is DBNull) nullCount++;
            else if (column.Nullable) validity[i / 8] |= (byte)(1 << (i % 8));
        }
        var validityBuffer = new ArrowBuffer(validity);
        if (column.FieldType == typeof(string))
        {
            int size = 0;
            foreach (var row in rows) if (row.Values[ordinal] is string s) size = checked(size + Utf8.GetByteCount(s));
            var data = new byte[size];
            var offsets = new byte[checked((count + 1) * 4)];
            int offset = 0;
            for (int i = 0; i < count; i++)
            {
                cancellation.ThrowIfCancellationRequested();
                if (rows[i].Values[ordinal] is string s) offset += Utf8.GetBytes(s.AsSpan(), data.AsSpan(offset));
                BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan((i + 1) * 4), offset);
            }
            return new StringArray(count, new ArrowBuffer(offsets), new ArrowBuffer(data), validityBuffer, nullCount, 0);
        }
        var values = new byte[checked(count * 8)];
        for (int i = 0; i < count; i++)
        {
            cancellation.ThrowIfCancellationRequested();
            object value = rows[i].Values[ordinal];
            if (value is DBNull) continue;
            long scalar = value switch {
                long n => n,
                double n => BitConverter.DoubleToInt64Bits(n),
                DateTime n => checked((n.Ticks - DateTime.UnixEpoch.Ticks) / 10),
                _ => throw new InvalidDataException("bulk.arrow_scalar") };
            BinaryPrimitives.WriteInt64LittleEndian(values.AsSpan(i * 8), scalar);
        }
        var buffer = new ArrowBuffer(values);
        return column.DataTypeName switch {
            "bigint" => new Int64Array(buffer, validityBuffer, count, nullCount, 0),
            "float(53)" => new DoubleArray(buffer, validityBuffer, count, nullCount, 0),
            _ => new TimestampArray(new TimestampType(TimeUnit.Microsecond, (string?)null), buffer, validityBuffer, count, nullCount, 0) };
    }

    private static IArrowType ArrowType(NativeColumn column) => column.DataTypeName switch {
        "bigint" => Int64Type.Default, "float(53)" => DoubleType.Default, "nvarchar(max)" => StringType.Default,
        _ => new TimestampType(TimeUnit.Microsecond, (string?)null) };

    public void Dispose()
    {
        pending?.Dispose();
        pending = null;
        decoder.Dispose();
    }
}
