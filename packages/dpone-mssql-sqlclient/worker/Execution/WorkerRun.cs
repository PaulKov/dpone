using System.Data.Common;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Execution.Input;
using Dpone.SqlClient.Execution.Session;
using Dpone.SqlClient.Input;
using Dpone.SqlClient.Job;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;
using Microsoft.Win32.SafeHandles;

namespace Dpone.SqlClient.Execution;

/// <summary>
/// Fixed post-startup composition for one Job, grant and bulk invocation. Parent
/// owns durable authority, physical settlement, verification and publication.
/// The first RunAsync caller exclusively owns its context; later callers cannot
/// access its descriptors or perform another execution.
/// </summary>
public sealed class WorkerRun
{
    private int started;
    private byte[]? retained;
    /// <summary>Independent copy of exact retained result bytes, never credential material.</summary>
    public byte[]? RetainedResult => Volatile.Read(ref retained)?.ToArray();

    /// <summary>
    /// Execute with original Linux deadlines after StartupBootstrap readiness.
    /// Frame I/O is bounded by that deadline; cancellation is checked around it.
    /// Parent containment remains required for blocked native/SDK calls and cleanup.
    /// Production composition passes allowDisposableTest=false.
    /// </summary>
    public async Task<int> RunAsync(StartupContext context, bool allowDisposableTest, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (Interlocked.Exchange(ref started, 1) != 0) throw new InvalidOperationException("worker.run_used");
        StartupLaunch launch = context.Launch;
        var deadline = new BulkDeadline(new SessionClock(), launch.OperationDeadlineNs);
        SafeFileHandle? resultHandle = null, inputHandle = null;
        SealedInputStream? stream = null;
        DbDataReader? reader = null;
        SingleWriterSession? session = null;
        string? acceptedGrant = null;
        bool resultAttempted = false;
        int exit = 1;
        SessionResultError phase = SessionResultError.Protocol;

        void Current() { cancellation.ThrowIfCancellationRequested(); _ = deadline.Remaining(); }
        SessionResultError Failure(Exception error, SessionResultError fallback)
        {
            if (error is OutOfMemoryException) return SessionResultError.ResourceLimit;
            if (error is TimeoutException) return SessionResultError.OperationTimeout;
            // Frame and SDK errors have different exception types. Observe the
            // original deadline; never assign a new budget to classify failure.
            try { _ = deadline.Remaining(); }
            catch (TimeoutException) { return SessionResultError.OperationTimeout; }
            catch (Exception) { return fallback; }
            return fallback;
        }
        void Cleanup(IDisposable? owned)
        {
            try { owned?.Dispose(); } catch (Exception) { exit = 1; }
        }
        void Emit(SessionResult result)
        {
            if (resultAttempted) throw new InvalidOperationException("worker.result_used");
            resultAttempted = true;
            byte[] bytes = result.Encode();
            Volatile.Write(ref retained, bytes);
            // Retention precedes delivery and every fallible close. No resend,
            // replacement result or fresh post-deadline delivery allowance.
            StartupFrame.Write(Fd(resultHandle!), bytes, launch.OperationDeadlineNs);
            resultHandle!.Dispose();
        }

        try
        {
            resultHandle = context.Take(StartupChannel.Result);
            Current();
            SqlClientJob job;
            using (SafeFileHandle credentials = context.Take(StartupChannel.Credentials))
            {
                byte[] body = StartupFrame.Read(Fd(credentials), launch.OperationDeadlineNs, SqlClientJob.MaxJobBytes);
                Current();
                job = SqlClientJob.Parse(body);
            }
            WorkerJobAdmission.RequireDeclaration(job, launch, allowDisposableTest, StartupClock.NowNs());
            phase = SessionResultError.Decoder;
            var policy = new BulkInputPolicy(job.BatchRows, job.MaxInputBatchBytes);
            var budget = new InputBufferBudget(policy.MaxInputBatchBytes);
            inputHandle = context.Take(StartupChannel.Input);
            stream = SealedInputStream.Admit(job.Input, Fd(inputHandle), deadline, ownsDescriptor: false, budget);
            Func<InputReceipt> complete;
            if (job.InputMode == "rows")
            {
                var rows = new RowsInput(stream, job.Input.Native, policy, budget, cancellation);
                reader = rows.Reader; complete = rows.Reader.RequireComplete;
            }
            else
            {
                var arrow = new BoundedArrowReader(stream, job.Input.Native, policy, budget, cancellation);
                reader = arrow; complete = arrow.RequireComplete;
            }

            if (job.Input.Native.Expected.Rows == 0)
            {
                if (reader.Read()) throw new InvalidDataException("worker.unexpected_empty_row");
                InputReceipt receipt = complete(); Current();
                Emit(new(launch.Digest, launch.AttemptSha256, null, receipt, null));
                exit = 0;
            }
            else
            {
                phase = SessionResultError.Connection;
                session = new SingleWriterSession(job.Credentials!, job.SessionNonce!, deadline, allowDisposableTest);
                OwnSqlSession initial = await session.OpenAsync(cancellation).ConfigureAwait(false);
                Current(); phase = SessionResultError.Protocol;
                using (SafeFileHandle announcement = context.Take(StartupChannel.Session))
                {
                    byte[] body = SessionControlCodec.EncodeAnnouncement(new(launch.Digest, launch.AttemptSha256,
                        initial.SessionId, initial.Nonce!));
                    StartupFrame.Write(Fd(announcement), body, launch.OperationDeadlineNs);
                }
                byte[] grantBody;
                using (SafeFileHandle grantHandle = context.Take(StartupChannel.Grant))
                    grantBody = StartupFrame.Read(Fd(grantHandle), launch.OperationDeadlineNs);
                Current(); phase = SessionResultError.Connection;
                OwnSqlSession current = await session.RequireSameAsync(cancellation).ConfigureAwait(false);
                phase = SessionResultError.Protocol;
                BulkGrant grant = new WorkerGrantAdmission().Accept(grantBody, launch, job, initial, current, StartupClock.NowNs());
                acceptedGrant = grant.GrantId;
                Current(); phase = SessionResultError.Driver;
                var destination = new BulkDestination(job.Identity.Database, job.Identity.Schema, job.Identity.Table);
                var writer = new ExistingConnectionBulkWriter(session.Connection, reader, job.Input.Native,
                    destination, policy, deadline, () =>
                    {
                        InputReceipt receipt = complete();
                        session.RequireSame(cancellation); Current();
                        return receipt;
                    });
                bool copyAccepted = false;
                await writer.WriteAsync(value =>
                {
                    SessionResult result;
                    try
                    {
                        Current();
                        if (value.Code != "copied_input_complete") throw new InvalidDataException("worker.copy_failed");
                        result = new(launch.Digest, launch.AttemptSha256, acceptedGrant, value.Input, null);
                        copyAccepted = true;
                    }
                    catch (Exception error)
                    {
                        result = new(launch.Digest, launch.AttemptSha256, acceptedGrant, null,
                            Failure(error, value.Code == "deadline_expired" ? SessionResultError.OperationTimeout : SessionResultError.Driver));
                    }
                    // Even cancellation/expiry must retain the failure before
                    // SqlBulkCopy disposal; delivery may fail under that deadline.
                    Emit(result);
                }, cancellation).ConfigureAwait(false);
                exit = copyAccepted ? 0 : 1;
            }
        }
        catch (Exception error)
        {
            if (!resultAttempted && resultHandle is not null)
            {
                SessionResultError code = Failure(error, phase);
                try { Emit(new(launch.Digest, launch.AttemptSha256, acceptedGrant, null, code)); }
                catch (Exception) { /* Missing/partial delivery is parent-owned uncertainty. */ }
            }
            exit = 1;
        }
        finally
        {
            // Attempt every cleanup even if an earlier resource close fails.
            Cleanup(reader); Cleanup(stream); Cleanup(session);
            Cleanup(inputHandle); Cleanup(resultHandle); Cleanup(context);
        }
        return exit;
    }

    private static int Fd(SafeFileHandle handle)
    {
        if (handle.IsClosed || handle.IsInvalid) throw new InvalidDataException("worker.channel_closed");
        return checked((int)handle.DangerousGetHandle());
    }
}
