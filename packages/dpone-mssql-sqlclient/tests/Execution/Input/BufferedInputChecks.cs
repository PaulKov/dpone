using System.Security.Cryptography;
using Dpone.SqlClient.Execution.Input;
using Dpone.SqlClient.Input;
using static SealedInputFixtures;

internal static class BufferedInputChecks
{
    internal static void ReadAhead(string directory)
    {
        byte[] body = Enumerable.Range(0, 16_385).SelectMany(i => BitConverter.GetBytes((long)i)).ToArray();
        string path = Path.Combine(directory, "buffered-native.bin"); File.WriteAllBytes(path, body);
        var budget = new InputBufferBudget(1 << 20);
        using (var raw = new RawDescriptor(path))
        {
            var descriptor = Descriptor(raw.Fd, body, Oracle(path));
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, budget);
            Assert(budget.UsedBytes == 65_536);
            byte[] actual = new byte[body.Length];
            Assert(stream.Read(Array.Empty<byte>(), 0, 0) == 0);
            Assert(RawDescriptor.lseek(raw.Fd, 0, 1) == 0);
            Assert(stream.Read(actual, 0, 1) == 1);
            Assert(RawDescriptor.lseek(raw.Fd, 0, 1) == 65_536);
            int offset = 1;
            while (offset < actual.Length)
            {
                int read = stream.Read(actual, offset, Math.Min(997, actual.Length - offset));
                Assert(read > 0); offset += read;
            }
            Assert(stream.Read(new byte[1], 0, 1) == 0);
            Assert(actual.SequenceEqual(body));
            Assert(SHA256.HashData(actual).SequenceEqual(SHA256.HashData(body)));
            Assert(budget.UsedBytes == 65_536);
        }
        Assert(budget.UsedBytes == 0);
        using (var raw = new RawDescriptor(path))
        {
            var descriptor = Descriptor(raw.Fd, body, Oracle(path));
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, budget);
            using var reader = new NativeInputReader(stream, descriptor.Native, default, budget);
            Assert(budget.UsedBytes == 65_537);
            long ordinal = 0;
            while (reader.Read())
            {
                Assert(reader.GetInt64(0) == ordinal++);
                Assert(budget.UsedBytes > 65_537 && budget.UsedBytes <= budget.LimitBytes);
            }
            Assert(ordinal == body.Length / 8 && reader.RequireComplete().Bytes == body.Length);
            Assert(reader.RequireComplete().Sha256 == Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant());
            reader.Dispose(); Assert(budget.UsedBytes == 65_536);
            stream.Dispose(); Assert(budget.UsedBytes == 0);
        }
        BudgetAndFailure(path, body);
        using (var raw = new RawDescriptor(path))
        {
            var descriptor = Descriptor(raw.Fd, body, Oracle(path));
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, budget);
            using var reader = new NativeInputReader(stream, descriptor.Native, default, budget);
            Assert(reader.Read()); Assert(RawDescriptor.lseek(raw.Fd, 0, 1) == 65_536);
            File.SetLastWriteTimeUtc(path, DateTime.UtcNow.AddDays(-2));
            Reject<InvalidDataException>(() => { while (reader.Read()) { } });
            Reject<InvalidDataException>(() => reader.RequireComplete());
            Assert(budget.UsedBytes == 65_536);
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
            Assert(budget.UsedBytes == 65_536);
        }
        Assert(budget.UsedBytes == 0);
        Console.WriteLine("PASS fixed64KiB kernel read-ahead, full content/hash/EOF, shared ledger and delayed drift rejection");
    }
    private static void BudgetAndFailure(string path, byte[] body)
    {
        using var raw = new RawDescriptor(path);
        var descriptor = Descriptor(raw.Fd, body, Oracle(path));
        var denied = new InputBufferBudget(65_535);
        using var prior = denied.Reserve(17);
        Reject<InvalidDataException>(() => SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), true, denied));
        Assert(denied.UsedBytes == 17 && RawDescriptor.fcntl(raw.Fd, 3) >= 0 && RawDescriptor.lseek(raw.Fd, 0, 1) == 0);
        var exact = new InputBufferBudget(65_536);
        using (var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, exact))
        {
            Assert(exact.UsedBytes == 65_536);
            Reject<InvalidDataException>(() => new NativeInputReader(stream, descriptor.Native, default, exact));
            Assert(exact.UsedBytes == 65_536);
            Reject<NotSupportedException>(() => stream.Flush());
            Assert(exact.UsedBytes == 65_536);
        }
        Assert(exact.UsedBytes == 0);
        var clock = new TestClock { ExpireOn = 3 };
        Reject<TimeoutException>(() => SealedInputStream.Admit(descriptor, raw.Fd, new(clock, 10_000_000_000), true, exact));
        Assert(exact.UsedBytes == 0 && RawDescriptor.fcntl(raw.Fd, 3) >= 0);
        foreach (int expires in new[] { 6, 7, 8 })
        {
            Assert(RawDescriptor.lseek(raw.Fd, 0, 0) == 0);
            clock = new TestClock { ExpireOn = expires };
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, new(clock, 10_000_000_000), false, exact);
            if (expires == 6) Reject<TimeoutException>(() => stream.Read(new byte[1], 0, 1));
            else
            {
                Assert(stream.Read(new byte[1], 0, 1) == 1);
                Reject<TimeoutException>(() => stream.Read(new byte[1], 0, 1));
            }
            Assert(exact.UsedBytes == 65_536);
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
            Assert(exact.UsedBytes == 65_536);
            stream.Dispose(); stream.Dispose(); Assert(exact.UsedBytes == 0);
        }
    }
}
