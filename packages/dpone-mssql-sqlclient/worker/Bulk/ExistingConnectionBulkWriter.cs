using System.Data;
using System.Data.Common;
using Dpone.SqlClient.Input;
using Microsoft.Data.SqlClient;

namespace Dpone.SqlClient.Bulk;

/// <summary>Component outcome only; neither durable parent evidence nor remote writer settlement.</summary>
public sealed record BulkCopyOutcome(string Code, InputReceipt? Input);

/// <summary>
/// One-shot thin SqlBulkCopy adapter over the caller's existing restricted session.
/// The caller owns grant/session/stage authority, the input and SQL connection,
/// durable result retention and independent remote writer settlement.
/// </summary>
public sealed class ExistingConnectionBulkWriter
{
    /// <summary>Fixed semantics for nulls, constraints, triggers, table locks and batch commits.</summary>
    public const SqlBulkCopyOptions RequiredOptions = SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.CheckConstraints |
        SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction;
    private readonly SqlConnection connection;
    private readonly DbDataReader reader;
    private readonly NativeInputContract contract;
    private readonly BulkDestination destination;
    private readonly BulkInputPolicy policy;
    private readonly BulkDeadline deadline;
    private readonly Func<InputReceipt> requireComplete;
    private int started;

    /// <summary>Bind already admitted resources without opening a connection or reading input.</summary>
    public ExistingConnectionBulkWriter(SqlConnection connection, DbDataReader reader, NativeInputContract contract,
        BulkDestination destination, BulkInputPolicy policy, BulkDeadline deadline, Func<InputReceipt> requireComplete)
    {
        this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        this.contract = contract ?? throw new ArgumentNullException(nameof(contract));
        this.destination = destination ?? throw new ArgumentNullException(nameof(destination));
        this.policy = policy ?? throw new ArgumentNullException(nameof(policy));
        this.deadline = deadline ?? throw new ArgumentNullException(nameof(deadline));
        this.requireComplete = requireComplete ?? throw new ArgumentNullException(nameof(requireComplete));
    }

    /// <summary>Reject closed or automatically managed connections; never open, reconnect or close them.</summary>
    public static void ValidateConnection(SqlConnection connection)
    {
        if (connection.State != ConnectionState.Open) throw new InvalidDataException("bulk.open_connection_required");
        var options = new SqlConnectionStringBuilder(connection.ConnectionString);
        if (options.Pooling || options.MultipleActiveResultSets || options.Enlist || options.ConnectRetryCount != 0)
            throw new InvalidDataException("bulk.connection_policy");
    }

    /// <summary>
    /// Copy once and synchronously retain the bounded outcome before fallible SDK
    /// disposal. Callback invocation is not itself proof that retention was durable.
    /// No success may be inferred from this result without parent barriers.
    /// </summary>
    public async Task<BulkCopyOutcome> WriteAsync(Action<BulkCopyOutcome> retain, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(retain);
        if (Interlocked.Exchange(ref started, 1) != 0) throw new InvalidOperationException("bulk.one_shot");
        SqlBulkCopy? copy = null;
        BulkCopyOutcome outcome;
        try
        {
            cancellation.ThrowIfCancellationRequested();
            ValidateConnection(connection);
            ValidateColumns();
            Guid originalConnection = connection.ClientConnectionId;
            copy = new SqlBulkCopy(connection, RequiredOptions, null)
            {
                DestinationTableName = destination.QualifiedName,
                EnableStreaming = true,
                BatchSize = policy.BatchRows,
                BulkCopyTimeout = deadline.SdkTimeoutSeconds()
            };
            for (int i = 0; i < contract.Columns.Count; i++) copy.ColumnMappings.Add(i, contract.Columns[i].Name);
            using var timer = deadline.CreateCancellation();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timer.Token);
            await copy.WriteToServerAsync(reader, linked.Token).ConfigureAwait(false);
            linked.Token.ThrowIfCancellationRequested();
            _ = deadline.Remaining();
            if (connection.State != ConnectionState.Open || connection.ClientConnectionId != originalConnection)
                throw new InvalidDataException("bulk.connection_changed");
            InputReceipt complete = requireComplete();
            if (complete.Rows != contract.Expected.Rows || complete.Bytes != contract.Expected.Bytes || complete.Sha256 != contract.Expected.Sha256)
                throw new InvalidDataException("bulk.input_receipt_mismatch");
            outcome = new BulkCopyOutcome("copied_input_complete", complete);
        }
        catch (OperationCanceledException) { outcome = new BulkCopyOutcome("copy_cancelled", TryInputReceipt()); }
        catch (TimeoutException) { outcome = new BulkCopyOutcome("deadline_expired", TryInputReceipt()); }
        catch (Exception) { outcome = new BulkCopyOutcome("copy_failed", TryInputReceipt()); }

        bool retained = false;
        try
        {
            retain(outcome);
            retained = true;
            return outcome;
        }
        finally
        {
            try { ((IDisposable?)copy)?.Dispose(); }
            catch (Exception) when (!retained) { /* Retention failure remains primary; no success is returned. */ }
            catch (Exception) { throw new InvalidOperationException("bulk.copy_dispose_failed"); }
        }
    }

    private void ValidateColumns()
    {
        if (reader.FieldCount != contract.Columns.Count) throw new InvalidDataException("bulk.columns");
        for (int i = 0; i < contract.Columns.Count; i++)
            if (reader.GetName(i) != contract.Columns[i].Name || reader.GetFieldType(i) != contract.Columns[i].FieldType)
                throw new InvalidDataException("bulk.columns");
    }

    private InputReceipt? TryInputReceipt()
    {
        try { return requireComplete(); }
        catch (Exception) { return null; }
    }
}
