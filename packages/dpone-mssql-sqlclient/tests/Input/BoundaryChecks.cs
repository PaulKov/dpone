using System.Security.Cryptography;
using System.Text;
using Dpone.SqlClient.Input;

internal static class BoundaryChecks
{
    internal static void Run()
    {
        NativeColumn text = NativeColumn.Admit("text", "nvarchar(max)", false, "nvarchar", 8, null, null, null, "utf-16le");
        byte[] value = Encoding.Unicode.GetBytes(new string('x', 100000));
        byte[] bytes = BitConverter.GetBytes((long)value.Length).Concat(value).ToArray();
        var contract = Contract(text, bytes, 1, bytes.Length);
        using (var fragmented = new ObservedStream(bytes, 3))
        using (var reader = new NativeInputReader(fragmented, contract))
        {
            Check(reader.HasRows && reader.HasRows && reader.Read(), "lookahead");
            Check(reader.GetString(0).Length == 100000, "fragmented_value");
            Check(!reader.Read() && reader.RequireComplete().Bytes == bytes.Length, "fragmented_eof");
            Check(fragmented.MaxRequest <= 65536 && fragmented.ReadCalls > 100, "bounded_read");
            reader.Close();
            Check(!fragmented.Disposed, "caller_ownership");
        }
        using (var oversized = new ObservedStream(BitConverter.GetBytes(long.MaxValue), 1))
        using (var reader = new NativeInputReader(oversized, Contract(text, oversized.Data, 1, 32)))
        {
            Reject(() => reader.Read());
            Check(oversized.Position == 8 && oversized.MaxRequest <= 8, "prefix_preallocation");
            Reject(() => reader.RequireComplete());
        }
        using (var early = new NativeInputReader(new MemoryStream(bytes), contract))
        {
            Check(early.HasRows, "prefetch");
            early.Close();
            Reject(() => early.RequireComplete());
        }
        using (var cancel = new CancellationTokenSource())
        using (var stream = new ObservedStream(bytes, 2, cancel))
        using (var reader = new NativeInputReader(stream, contract, cancel.Token))
        {
            try { reader.Read(); throw new Exception("cancellation_missing"); }
            catch (OperationCanceledException) { }
            Reject(() => reader.RequireComplete());
            Reject(() => reader.Read());
        }
        using (var reader = new NativeInputReader(new MemoryStream(bytes), contract))
        {
            _ = reader.HasRows;
            try { reader.ReadAsync(new CancellationToken(true)).GetAwaiter().GetResult(); throw new Exception("async_cancel_missing"); }
            catch (OperationCanceledException) { }
            Reject(() => reader.Read());
            Reject(() => reader.RequireComplete());
        }
        foreach (bool asynchronous in new[] { false, true })
        {
            using var originalCancellation = new CancellationTokenSource();
            using var input = new MemoryStream(bytes);
            using var reader = new NativeInputReader(input, contract, originalCancellation.Token);
            Check(reader.HasRows, "constructor_token_prefetch");
            originalCancellation.Cancel();
            try
            {
                if (asynchronous) reader.ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
                else reader.Read();
                throw new Exception("constructor_cancel_missing");
            }
            catch (OperationCanceledException) { }
            Reject(() => reader.Read());
            Reject(() => reader.RequireComplete());
            Check(input.CanRead, "cancel_preserves_caller_stream");
        }
        using (var originalCancellation = new CancellationTokenSource())
        using (var reader = new NativeInputReader(new MemoryStream(Array.Empty<byte>()),
            Contract(text, Array.Empty<byte>(), 0, 32), originalCancellation.Token))
        {
            Check(!reader.Read(), "empty_complete");
            var completed = reader.RequireComplete();
            originalCancellation.Cancel();
            try { reader.Read(); throw new Exception("ended_cancel_missing"); }
            catch (OperationCanceledException) { }
            Reject(() => reader.Read());
            Check(reader.RequireComplete() == completed, "completed_receipt_remains_immutable");
        }
        using (var reader = new NativeInputReader(new LeakyStream(), contract))
        {
            try { reader.Read(); throw new Exception("expected_io_rejection"); }
            catch (InvalidDataException e) { Check(!e.Message.Contains("private-business"), "stream_error_redaction"); }
            Reject(() => reader.RequireComplete());
        }
        using (var reader = new NativeInputReader(new MemoryStream(bytes.Concat(bytes).ToArray()), contract))
        {
            Check(reader.Read(), "first_extra_row");
            Reject(() => reader.Read());
            Reject(() => reader.RequireComplete());
        }
        var columns = new[] { text };
        var snapshot = new NativeInputContract(columns, contract.Expected, contract.Limits);
        columns[0] = NativeColumn.Admit("other", "bigint", false, "bigint", 0, 8, null, null, null);
        Check(snapshot.Columns[0].Name == "text", "immutable_columns");
        Reject(() => new NativeInputContract(Enumerable.Repeat(text, 101), contract.Expected, contract.Limits));
        Reject(() => new NativeInputContract(new[] { text, text }, contract.Expected, contract.Limits));
        Reject(() => NativeColumn.Admit("x", "nvarchar(max) nullable", false, "nvarchar", 8, null, null, null, "utf-16le"));
        Reject(() => new ExpectedInput(-1, 0, new string('0', 64)));
        Reject(() => new ExpectedInput(0, 0, new string('A', 64)));
        Check(new ExpectedInput(long.MaxValue, long.MaxValue, new string('0', 64)).Rows == long.MaxValue, "int64_expected");
        Console.WriteLine("PASS boundary controls: fragmented reads, bounded requests/allocation, cancellation, early close, immutable metadata, int64 admission");
    }
    private static NativeInputContract Contract(NativeColumn c, byte[] data, long rows, int bound) => new(new[] { c },
        new ExpectedInput(rows, data.LongLength, Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant()), new NativeInputLimits(bound));
    private static void Check(bool condition, string code) { if (!condition) throw new Exception(code); }
    private static void Reject(Action action)
    {
        try { action(); }
        catch (InvalidDataException) { return; }
        catch (ObjectDisposedException) { return; }
        throw new Exception("expected_rejection");
    }
    private sealed class LeakyStream : MemoryStream
    {
        public override int Read(byte[] buffer, int offset, int count) => throw new InvalidDataException("private-business-value");
    }
    private sealed class ObservedStream : Stream
    {
        internal readonly byte[] Data;
        internal int MaxRequest, ReadCalls;
        internal bool Disposed;
        private readonly int fragment;
        private readonly CancellationTokenSource? cancel;
        internal ObservedStream(byte[] data, int fragment, CancellationTokenSource? cancel = null)
            => (Data, this.fragment, this.cancel) = (data, fragment, cancel);
        public override int Read(byte[] buffer, int offset, int count)
        {
            MaxRequest = Math.Max(MaxRequest, count);
            ReadCalls++;
            int size = (int)Math.Min(Math.Min(fragment, count), Data.LongLength - Position);
            Array.Copy(Data, Position, buffer, offset, size);
            Position += size;
            if (ReadCalls == 3) cancel?.Cancel();
            return size;
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get; set; }
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        protected override void Dispose(bool disposing) { Disposed = true; base.Dispose(disposing); }
    }
}
