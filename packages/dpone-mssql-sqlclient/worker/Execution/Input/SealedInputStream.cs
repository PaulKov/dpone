using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Input;
using Microsoft.Win32.SafeHandles;

namespace Dpone.SqlClient.Execution.Input;

/// <summary>
/// One-shot forward-only Linux native input stream. Admission observes a read-only
/// regular descriptor at offset zero; natural EOF rechecks its original fstat identity
/// before exposing zero bytes. NativeInputReader still owns format/hash/count proof.
/// Deadline checks surround synchronous syscalls; supervisor process containment
/// remains responsible for a syscall that never returns. No retries are performed.
/// </summary>
public sealed class SealedInputStream : Stream
{
    private readonly object gate = new();
    private readonly SafeFileHandle handle;
    private readonly int fd;
    private readonly JobFileIdentity original;
    private readonly BulkDeadline deadline;
    private const int BufferBytes = 65_536;
    private readonly InputBufferReservation reservation;
    private byte[]? readBuffer;
    private int bufferOffset, bufferCount;
    private bool disposed, failed, eof;
    private SealedInputStream(int fd, JobFileIdentity original, BulkDeadline deadline, bool ownsDescriptor,
        byte[] readBuffer, InputBufferReservation reservation)
    {
        this.fd = fd; this.original = original; this.deadline = deadline;
        this.readBuffer = readBuffer; this.reservation = reservation;
        handle = new SafeFileHandle((IntPtr)fd, ownsDescriptor);
    }
    /// <summary>
    /// Admit the declared descriptor against an independently trusted original FD and
    /// deadline. Ownership transfers only on successful return when ownsDescriptor is
    /// true. Failed admission never closes an FD. If false, disposal leaves it open.
    /// Caller must retain exclusive read/offset/close control for the stream lifetime;
    /// concurrent foreign close, reuse or alias reads violate the admitted ownership.
    /// A fixed 64KiB buffer is reserved before allocation in the same ledger supplied
    /// to the decoder and Arrow consumers. It remains charged through failure until
    /// disposal clears the buffer reference. Admission failure releases its reservation.
    /// This does not create a kernel seal or change file permissions.
    /// </summary>
    public static SealedInputStream Admit(JobInputDescriptor descriptor, int expectedFd, BulkDeadline deadline, bool ownsDescriptor, InputBufferBudget budget)
    {
        ArgumentNullException.ThrowIfNull(descriptor); ArgumentNullException.ThrowIfNull(deadline);
        ArgumentNullException.ThrowIfNull(budget);
        deadline.Remaining(); LinuxInputHandle.Platform();
        if (expectedFd < 3 || descriptor.Fd != expectedFd) throw LinuxInputHandle.Invalid();
        LinuxInputHandle.RequireStart(expectedFd);
        if (LinuxInputHandle.Observe(expectedFd) != descriptor.FileIdentity) throw LinuxInputHandle.Invalid();
        deadline.Remaining();
        InputBufferReservation reservation = budget.Reserve(BufferBytes);
        byte[]? buffer = null;
        try
        {
            buffer = new byte[BufferBytes];
            deadline.Remaining();
            return new(expectedFd, descriptor.FileIdentity, deadline, ownsDescriptor, buffer, reservation);
        }
        catch
        {
            buffer = null;
            reservation.Dispose();
            throw;
        }
    }
    /// <inheritdoc/>
    public override bool CanRead => !disposed && !failed;
    /// <inheritdoc/>
    public override bool CanSeek => false;
    /// <inheritdoc/>
    public override bool CanWrite => false;
    /// <inheritdoc/>
    public override long Length => throw Misuse();
    /// <inheritdoc/>
    public override long Position { get => throw Misuse(); set => throw Misuse(); }
    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        lock (gate)
        {
            RequireOpen();
            try
            {
                ArgumentNullException.ThrowIfNull(buffer);
                if (offset < 0 || count < 0 || offset > buffer.Length - count) throw new ArgumentOutOfRangeException(nameof(offset));
                deadline.Remaining();
                if (count == 0) { deadline.Remaining(); return 0; } // Caller asked for no bytes; not natural EOF.
                if (eof) { deadline.Remaining(); return 0; }
                if (bufferOffset == bufferCount)
                {
                    bufferOffset = 0;
                    bufferCount = LinuxInputHandle.Read(handle, readBuffer!, 0, BufferBytes);
                    deadline.Remaining();
                    if (bufferCount == 0)
                    {
                        if (LinuxInputHandle.Observe(fd) != original) throw LinuxInputHandle.Invalid();
                        deadline.Remaining();
                        eof = true;
                        return 0;
                    }
                }
                int copied = Math.Min(count, bufferCount - bufferOffset);
                readBuffer!.AsSpan(bufferOffset, copied).CopyTo(buffer.AsSpan(offset, copied));
                bufferOffset += copied;
                deadline.Remaining();
                return copied;
            }
            catch { failed = true; throw; }
        }
    }
    /// <inheritdoc/>
    public override void Flush() => throw Misuse();
    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin) => throw Misuse();
    /// <inheritdoc/>
    public override void SetLength(long value) => throw Misuse();
    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count) => throw Misuse();
    private NotSupportedException Misuse()
    {
        lock (gate) { failed = true; return new NotSupportedException("input.sealed_forward_read_only"); }
    }
    private void RequireOpen()
    {
        if (disposed) throw new ObjectDisposedException(nameof(SealedInputStream));
        if (failed) throw new InvalidDataException("input.sealed_poisoned");
    }
    /// <summary>Clear the buffered data reference before releasing its charge; close only a transferred owned FD.</summary>
    protected override void Dispose(bool disposing)
    {
        lock (gate)
        {
            if (!disposed)
            {
                disposed = true;
                readBuffer = null;
                bufferOffset = bufferCount = 0;
                reservation.Dispose();
                handle.Dispose();
            }
        }
        base.Dispose(disposing);
    }
}
