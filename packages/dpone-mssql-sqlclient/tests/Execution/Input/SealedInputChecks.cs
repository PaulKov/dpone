using Dpone.SqlClient.Execution.Input;
using Dpone.SqlClient.Input;
using Dpone.SqlClient.Bulk;
using static SealedInputFixtures;

internal static class SealedInputChecks
{
    internal static void Admission(string path, byte[] bytes)
    {
        using var raw = new RawDescriptor(path);
        var observed = Oracle(path); var descriptor = Descriptor(raw.Fd, bytes, observed);
        foreach (var identity in new[] { observed with { Device = observed.Device ^ 1 }, observed with { Inode = observed.Inode ^ 1 },
            observed with { MtimeNs = observed.MtimeNs + 1 }, observed with { CtimeNs = observed.CtimeNs + 1 } })
            Reject<InvalidDataException>(() => SealedInputStream.Admit(Descriptor(raw.Fd, bytes, identity), raw.Fd, Deadline(), true, new InputBufferBudget(1 << 20)));
        Reject<InvalidDataException>(() => SealedInputStream.Admit(descriptor, raw.Fd + 1, Deadline(), true, new InputBufferBudget(1 << 20)));
        Reject<InvalidDataException>(() => SealedInputStream.Admit(descriptor, 2, Deadline(), true, new InputBufferBudget(1 << 20)));
        using (var foreign = new RawDescriptor(path))
        {
            Reject<InvalidDataException>(() => SealedInputStream.Admit(descriptor, foreign.Fd, Deadline(), true, new InputBufferBudget(1 << 20)));
            Assert(RawDescriptor.fcntl(foreign.Fd, 3) >= 0);
        }
        Reject<InvalidDataException>(() => SealedInputStream.Admit(Descriptor(raw.Fd, new byte[16], observed with { Size = 16 }), raw.Fd, Deadline(), true, new InputBufferBudget(1 << 20)));
        Assert(RawDescriptor.fcntl(raw.Fd, 3) >= 0);
        Assert(RawDescriptor.lseek(raw.Fd, 1, 0) == 1);
        Reject<InvalidDataException>(() => SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), true, new InputBufferBudget(1 << 20)));
        Assert(RawDescriptor.lseek(raw.Fd, 0, 0) == 0);
        foreach (int flags in new[] { 1, 2, 0x200000 })
        {
            using var other = new RawDescriptor(path, flags);
            Reject<InvalidDataException>(() => SealedInputStream.Admit(Descriptor(other.Fd, bytes, observed), other.Fd, Deadline(), true, new InputBufferBudget(1 << 20)));
            Assert(RawDescriptor.fcntl(other.Fd, 3) >= 0);
        }
        using (var folder = new RawDescriptor(Path.GetDirectoryName(path)!))
            Reject<InvalidDataException>(() => SealedInputStream.Admit(Descriptor(folder.Fd, bytes, observed), folder.Fd, Deadline(), true, new InputBufferBudget(1 << 20)));
        var pipe = new int[2]; Assert(RawDescriptor.pipe(pipe) == 0);
        try
        {
            Reject<InvalidDataException>(() => SealedInputStream.Admit(Descriptor(pipe[0], bytes, observed), pipe[0], Deadline(), true, new InputBufferBudget(1 << 20)));
            Assert(RawDescriptor.fcntl(pipe[0], 3) >= 0);
        }
        finally { RawDescriptor.close(pipe[0]); RawDescriptor.close(pipe[1]); }
        var clock = new TestClock { Now = 10_000_000_000 };
        Reject<TimeoutException>(() => SealedInputStream.Admit(descriptor, raw.Fd, new(clock, 10_000_000_000), true, new InputBufferBudget(1 << 20)));
        clock = new TestClock { ExpireOn = 2 };
        Reject<TimeoutException>(() => SealedInputStream.Admit(descriptor, raw.Fd, new(clock, 10_000_000_000), true, new InputBufferBudget(1 << 20)));
        Assert(RawDescriptor.fcntl(raw.Fd, 3) >= 0);
        Console.WriteLine("PASS admission identity, FD/access/type/offset, deadline and rejected-FD ownership");
    }
    internal static void Drift(string directory, byte[] bytes)
    {
        foreach (string kind in new[] { "size", "mtime", "ctime", "inode" })
        {
            string path = Path.Combine(directory, "drift-" + kind); File.WriteAllBytes(path, bytes);
            using var raw = new RawDescriptor(path);
            var observed = Oracle(path); var descriptor = Descriptor(raw.Fd, bytes, observed);
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, new InputBufferBudget(1 << 20));
            using var reader = new NativeInputReader(stream, descriptor.Native);
            Assert(reader.Read());
            switch (kind)
            {
                case "size": using (var writer = new FileStream(path, FileMode.Open, FileAccess.Write)) writer.SetLength(0); break;
                case "mtime": File.SetLastWriteTimeUtc(path, DateTime.UtcNow.AddDays(-1)); break;
                case "ctime": Assert(RawDescriptor.chmod(path, 0x100) == 0); break; // Owner-read only on synthetic fixture.
                case "inode":
                    // Adversarial violation of exclusive FD ownership: reject the changed incarnation.
                    string replacement = path + "-replacement"; File.WriteAllBytes(replacement, bytes);
                    using (var changed = new RawDescriptor(replacement))
                    {
                        Assert(RawDescriptor.lseek(changed.Fd, 8, 0) == 8);
                        Assert(RawDescriptor.dup2(changed.Fd, raw.Fd) == raw.Fd);
                    }
                    break;
            }
            Reject<InvalidDataException>(() => reader.Read());
            Reject<InvalidDataException>(() => reader.RequireComplete());
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
            Assert(!stream.CanRead);
        }
        string empty = Path.Combine(directory, "empty"); File.WriteAllBytes(empty, Array.Empty<byte>());
        using (var raw = new RawDescriptor(empty))
        {
            var descriptor = Descriptor(raw.Fd, Array.Empty<byte>(), Oracle(empty));
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, new InputBufferBudget(1 << 20));
            Assert(stream.Read(Array.Empty<byte>(), 0, 0) == 0);
            using var reader = new NativeInputReader(stream, descriptor.Native);
            Assert(!reader.Read()); Assert(reader.RequireComplete().Rows == 0 && reader.RequireComplete().Bytes == 0);
        }
        using (var raw = new RawDescriptor(empty))
        {
            var descriptor = Descriptor(raw.Fd, Array.Empty<byte>(), Oracle(empty));
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, new InputBufferBudget(1 << 20));
            Assert(stream.Read(Array.Empty<byte>(), 0, 0) == 0);
            File.WriteAllBytes(empty, bytes);
            using var reader = new NativeInputReader(stream, descriptor.Native);
            Reject<InvalidDataException>(() => reader.Read());
            Reject<InvalidDataException>(() => reader.RequireComplete());
        }
        Console.WriteLine("PASS natural EOF receipts, drift prevents receipt, and zero-length read is not EOF");
    }
    internal static void Lifetime(string path, byte[] bytes)
    {
        using (var raw = new RawDescriptor(path))
        {
            int fd = raw.Fd;
            var stream = SealedInputStream.Admit(Descriptor(fd, bytes, Oracle(path)), fd, Deadline(), true, new InputBufferBudget(1 << 20));
            raw.Transferred(); stream.Dispose(); stream.Dispose();
            Assert(RawDescriptor.fcntl(fd, 3) == -1);
            Reject<ObjectDisposedException>(() => stream.Read(new byte[1], 0, 1));
        }
        foreach (Action<Stream> misuse in new Action<Stream>[] { s => s.Write(new byte[1], 0, 1), s => s.Seek(0, SeekOrigin.Begin),
            s => s.SetLength(0), s => s.Flush(), s => { _ = s.Length; }, s => s.Position = 0 })
        {
            using var raw = new RawDescriptor(path);
            using var stream = SealedInputStream.Admit(Descriptor(raw.Fd, bytes, Oracle(path)), raw.Fd, Deadline(), false, new InputBufferBudget(1 << 20));
            Reject<NotSupportedException>(() => misuse(stream));
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
        }
        foreach (int expireOn in new[] { 4, 5, 6 })
        {
            using var raw = new RawDescriptor(path); var clock = new TestClock { ExpireOn = expireOn };
            using var stream = SealedInputStream.Admit(Descriptor(raw.Fd, bytes, Oracle(path)), raw.Fd, new(clock, 10_000_000_000), false, new InputBufferBudget(1 << 20));
            Reject<TimeoutException>(() => stream.Read(new byte[8], 0, 8));
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
        }
        using (var raw = new RawDescriptor(path))
        {
            var clock = new TestClock { ExpireOn = 9 };
            var descriptor = Descriptor(raw.Fd, bytes, Oracle(path));
            using var stream = SealedInputStream.Admit(descriptor, raw.Fd, new(clock, 10_000_000_000), false, new InputBufferBudget(1 << 20));
            byte[] target = Enumerable.Repeat((byte)99, 10).ToArray();
            Assert(stream.Read(target, 1, 8) == 8 && target[0] == 99 && target[9] == 99);
            Assert(target.AsSpan(1, 8).SequenceEqual(bytes));
            Reject<TimeoutException>(() => stream.Read(new byte[1], 0, 1));
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
        }
        using (var raw = new RawDescriptor(path))
        {
            using var stream = SealedInputStream.Admit(Descriptor(raw.Fd, bytes, Oracle(path)), raw.Fd, Deadline(), false, new InputBufferBudget(1 << 20));
            Reject<ArgumentOutOfRangeException>(() => stream.Read(new byte[1], 1, 1));
            Reject<InvalidDataException>(() => stream.Read(new byte[1], 0, 1));
        }
        Console.WriteLine("PASS disposal ownership, capability misuse, original read/EOF deadline poisoning and offset reads");
    }
}
