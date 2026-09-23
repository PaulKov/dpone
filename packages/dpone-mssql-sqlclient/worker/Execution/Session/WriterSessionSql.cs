using System.Data;
using System.Data.Common;
using Dpone.SqlClient.Bulk;
using Microsoft.Data.SqlClient;

namespace Dpone.SqlClient.Execution.Session;

internal static class WriterSessionSql
{
    // Current-session and current-principal rows require no broad server-state grant.
    // CONTEXT_INFO() may pad to128 bytes; the DMV column preserves exact stored length.
    // Canonical Int32 projection: SQL built-ins can return smallint, which SqlClient
    // correctly materializes as Int16. Keep the decoder exact instead of accepting aliases.
    // No sys.dm_exec_connections or substituted connect_time/client authority is used.
    internal const string Self = """
        SELECT CAST(@@SPID AS int), s.context_info, CAST(DB_ID() AS int), DB_NAME(),
        SUSER_SNAME(), SUSER_SID(), ORIGINAL_LOGIN(), SUSER_SID(ORIGINAL_LOGIN()),
        CAST(USER_ID() AS int), USER_NAME(), p.sid, s.login_time,
        CAST(@@TRANCOUNT AS int), CAST(XACT_STATE() AS int), CAST((@@OPTIONS & 2) AS int)
        FROM sys.database_principals AS p CROSS JOIN sys.dm_exec_sessions AS s
        WHERE p.principal_id = USER_ID() AND s.session_id = @@SPID;
        """;
    private static SqlCommand Command(SqlConnection connection, string sql, BulkDeadline deadline)
    {
        var command = new SqlCommand(sql, connection) { CommandTimeout = deadline.SdkTimeoutSeconds() };
        command.RetryLogicProvider = SqlConfigurableRetryFactory.CreateNoneRetryProvider();
        return command;
    }
    internal static async Task<OwnSqlSession> Observe(SqlConnection connection, BulkDeadline deadline, CancellationToken cancellation)
    {
        _ = deadline.Remaining(); cancellation.ThrowIfCancellationRequested();
        Guid before = connection.ClientConnectionId;
        using var command = Command(connection, Self, deadline);
        using var timer = deadline.CreateCancellation();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timer.Token);
        using var reader = await command.ExecuteReaderAsync(CommandBehavior.Default, linked.Token).ConfigureAwait(false);
        OwnSqlSession observed = await ReadOne(reader, before, linked.Token).ConfigureAwait(false);
        linked.Token.ThrowIfCancellationRequested(); _ = deadline.Remaining();
        if (connection.State != ConnectionState.Open || before != connection.ClientConnectionId) throw SessionFailure.Invalid();
        return observed;
    }
    internal static async Task<OwnSqlSession> ReadOne(DbDataReader reader, Guid clientConnectionId, CancellationToken cancellation)
    {
        if (reader.FieldCount != 15 || !await reader.ReadAsync(cancellation).ConfigureAwait(false)) throw SessionFailure.Invalid();
        var row = new object[15];
        if (reader.GetValues(row) != 15 || await reader.ReadAsync(cancellation).ConfigureAwait(false) ||
            await reader.NextResultAsync(cancellation).ConfigureAwait(false)) throw SessionFailure.Invalid();
        cancellation.ThrowIfCancellationRequested();
        return OwnSqlSession.Decode(row, clientConnectionId);
    }
    internal static async Task SetNonce(SqlConnection connection, string nonce, BulkDeadline deadline, CancellationToken cancellation)
    {
        _ = deadline.Remaining(); cancellation.ThrowIfCancellationRequested();
        using var command = Command(connection, "SET CONTEXT_INFO @nonce;", deadline);
        command.Parameters.Add("@nonce", SqlDbType.VarBinary, 32).Value = Convert.FromHexString(nonce);
        using var timer = deadline.CreateCancellation();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timer.Token);
        if (await command.ExecuteNonQueryAsync(linked.Token).ConfigureAwait(false) != -1) throw SessionFailure.Invalid();
        linked.Token.ThrowIfCancellationRequested(); _ = deadline.Remaining();
    }
}
