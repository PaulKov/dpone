using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Dpone.SqlClient.Input;

/// <summary>
/// Forward-only native decoder over an injected caller-owned stream. The caller
/// authenticates and pins the descriptor/handle; this component never opens paths,
/// seeks, retries, closes the stream, or treats its receipt as SQL delivery proof.
/// </summary>
public sealed class NativeRowDecoder : IDisposable
{
    private readonly Stream stream;
    private readonly NativeInputContract contract;
    private readonly CancellationToken cancellation;
    private readonly IncrementalHash hash;
    private long bytes, rows;
    private int rowBytes;
    private bool failed, disposed;
    private InputReceipt? receipt;
    private byte[] single;
    private readonly InputBufferBudget? budget;
    private readonly InputBufferReservation? baseline;

    /// <summary>Bind the supplied stream once; ownership and position remain with its caller.</summary>
    public NativeRowDecoder(Stream stream, NativeInputContract contract, CancellationToken cancellation = default)
        : this(stream, contract, cancellation, null) { }

    /// <summary>Bind a shared budget; all retained rows require the lease API.</summary>
    public NativeRowDecoder(Stream stream, NativeInputContract contract, CancellationToken cancellation, InputBufferBudget? budget)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(contract);
        if (!stream.CanRead) throw new InvalidDataException("input.unreadable_stream");
        (this.stream, this.contract, this.cancellation, this.budget) = (stream, contract, cancellation, budget);
        baseline = budget?.Reserve(1);
        try
        {
            single = new byte[1];
            hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        }
        catch { baseline?.Dispose(); throw; }
    }

    /// <summary>Legacy unbudgeted row array; budgeted callers must use ReadNextLease.</summary>
    public object[]? ReadNext(CancellationToken cancellationToken = default)
    {
        if (budget is not null) throw new InvalidOperationException("input.lease_required");
        using DecodedRowLease? row = ReadNextLease(cancellationToken);
        return row?.DetachUnbudgetedValues();
    }

    /// <summary>Decode with exclusive row ownership; caller must dispose each returned lease.</summary>
    public DecodedRowLease? ReadNextLease(CancellationToken cancellationToken = default)
    {
        if (disposed) throw new ObjectDisposedException(nameof(NativeRowDecoder));
        if (failed) throw new InvalidDataException("input.failed_reader");
        if (receipt is not null) return null;
        DecodedRowLease? result = null;
        try
        {
            CheckCancellation(cancellationToken);
            rowBytes = 0;
            int count = ReadStream(single, 0, 1, cancellationToken);
            if (count == 0)
            {
                string digest = Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
                if (bytes != contract.Expected.Bytes || rows != contract.Expected.Rows || digest != contract.Expected.Sha256)
                    throw new InvalidDataException("input.completeness_mismatch");
                receipt = new InputReceipt(rows, bytes, digest);
                return null;
            }
            Observe(single.AsSpan(0, 1));
            if (rows >= contract.Expected.Rows || bytes > contract.Expected.Bytes)
                throw new InvalidDataException("input.extra_content");
            result = DecodedRowLease.Create(contract.Columns.Count, budget);
            bool first = true;
            for (int ordinal = 0; ordinal < contract.Columns.Count; ordinal++)
            {
                CheckCancellation(cancellationToken);
                NativeColumn column = contract.Columns[ordinal];
                long length = column.FixedLength ?? 0;
                if (column.PrefixWidth > 0)
                {
                    using (InputBufferReservation? prefixCharge = budget?.Reserve(column.PrefixWidth))
                    {
                        byte[] prefix = ReadExact(column.PrefixWidth, ref first, cancellationToken);
                        length = prefix.Length == 1 ? (sbyte)prefix[0] : BinaryPrimitives.ReadInt64LittleEndian(prefix);
                    }
                    if (length == -1)
                    {
                        if (!column.Nullable) throw new InvalidDataException("input.unexpected_null");
                        result.Values[ordinal] = DBNull.Value;
                        continue;
                    }
                }
                if (length < 0 || column.FixedLength is int fixedSize && length != fixedSize)
                    throw new InvalidDataException("input.invalid_length");
                if (column.DataTypeName == "nvarchar(max)" && length % 2 != 0)
                    throw new InvalidDataException("input.invalid_utf16");
                ValidateLength(length, first);
                using (InputBufferReservation? scratch = budget?.Reserve(length))
                {
                    byte[] payload = ReadExact(length, ref first, cancellationToken);
                    InputBufferReservation? retained = budget?.Reserve(column.DataTypeName == "nvarchar(max)" ? length : 8);
                    try
                    {
                        CheckCancellation(cancellationToken);
                        result.Values[ordinal] = NativeScalars.Decode(column, payload);
                        CheckCancellation(cancellationToken);
                        result.Retain(retained);
                        retained = null;
                    }
                    finally
                    {
                        if (retained is not null)
                        {
                            result.Values[ordinal] = null!;
                            retained.Dispose();
                        }
                    }
                }
            }
            rows = checked(rows + 1);
            return result;
        }
        catch (OperationCanceledException) { Fail(result); throw; }
        catch (InvalidDataException) { Fail(result); throw; }
        catch (Exception) { Fail(result); throw new InvalidDataException("input.read_failed"); }
    }

    private void Fail(DecodedRowLease? row)
    {
        failed = true;
        row?.Dispose();
        Dispose();
    }

    private void ValidateLength(long length, bool first)
    {
        // The first byte was probed/hashed already; include it in the remaining
        // file allowance only until it has been copied into this first field.
        long available = contract.Expected.Bytes - bytes + (first ? 1 : 0);
        if (length < 0 || length > contract.Limits.MaxRowBytes - rowBytes || length > available)
            throw new InvalidDataException("input.allocation_bound");
    }

    private byte[] ReadExact(long length, ref bool first, CancellationToken token)
    {
        ValidateLength(length, first);
        var buffer = new byte[checked((int)length)];
        int offset = 0;
        if (first && buffer.Length > 0)
        {
            buffer[0] = single[0];
            first = false;
            offset = 1;
        }
        while (offset < buffer.Length)
        {
            CheckCancellation(token);
            int count = ReadStream(buffer, offset, Math.Min(65536, buffer.Length - offset), token);
            if (count == 0) throw new InvalidDataException("input.truncated");
            Observe(buffer.AsSpan(offset, count));
            offset += count;
        }
        rowBytes = checked(rowBytes + buffer.Length);
        return buffer;
    }

    private int ReadStream(byte[] buffer, int offset, int count, CancellationToken token)
    {
        CheckCancellation(token);
        int read;
        try { read = stream.Read(buffer, offset, count); }
        catch (OperationCanceledException) { throw new OperationCanceledException("input.cancelled", token); }
        catch (Exception) { throw new InvalidDataException("input.stream_read_failed"); }
        if (read < 0 || read > count) throw new InvalidDataException("input.invalid_stream_read");
        CheckCancellation(token);
        return read;
    }

    private void Observe(ReadOnlySpan<byte> value)
    {
        bytes = checked(bytes + value.Length);
        hash.AppendData(value);
    }

    private void CheckCancellation(CancellationToken token)
    {
        cancellation.ThrowIfCancellationRequested();
        token.ThrowIfCancellationRequested();
    }

    /// <summary>Return the immutable receipt only after validated natural EOF.</summary>
    public InputReceipt RequireComplete() => receipt ?? throw new InvalidDataException("input.incomplete");

    /// <summary>Release hashing resources; never close the injected stream or manufacture EOF.</summary>
    public void Dispose()
    {
        if (disposed) return;
        disposed = true;
        hash.Dispose();
        single = Array.Empty<byte>();
        baseline?.Dispose();
    }
}
