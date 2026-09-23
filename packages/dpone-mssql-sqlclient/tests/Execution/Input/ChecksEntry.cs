using Dpone.SqlClient.Execution.Input;
using Dpone.SqlClient.Input;
using static SealedInputFixtures;

string directory = Path.Combine(args[0], "synthetic-files-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(directory);
string path = Path.Combine(directory, "native.bin");
byte[] bytes = BitConverter.GetBytes(42L);
File.WriteAllBytes(path, bytes);
using (var raw = new RawDescriptor(path))
{
    var observed = Oracle(path);
    var actual = LinuxInputHandle.Observe(raw.Fd);
    Assert(actual == observed);
    File.WriteAllText(Path.Combine(args[0], "buffered-independent-stat-observation.json"),
        System.Text.Json.JsonSerializer.Serialize(new { oracle = "GNU stat", expected = observed, actual }));
    Console.WriteLine("PASS independent GNU stat device/inode/size/mtime_ns/ctime_ns parity");
    var descriptor = Descriptor(raw.Fd, bytes, observed);
    using var stream = SealedInputStream.Admit(descriptor, raw.Fd, Deadline(), false, new InputBufferBudget(1 << 20));
    Assert(stream.CanRead && !stream.CanSeek && !stream.CanWrite);
    using var reader = new NativeInputReader(stream, descriptor.Native);
    Assert(reader.Read() && reader.GetInt64(0) == 42);
    Assert(!reader.Read()); Assert(reader.RequireComplete().Rows == 1);
    Assert(reader.RequireComplete().Bytes == 8);
    reader.Dispose(); Assert(RawDescriptor.fcntl(raw.Fd, 3) >= 0);
    stream.Dispose(); stream.Dispose(); Assert(RawDescriptor.fcntl(raw.Fd, 3) >= 0);
}
BufferedInputChecks.ReadAhead(directory);
SealedInputChecks.Admission(path, bytes);
SealedInputChecks.Drift(directory, bytes);
SealedInputChecks.Lifetime(path, bytes);
Console.WriteLine("PASS sealed-input assertions=" + Checks);
