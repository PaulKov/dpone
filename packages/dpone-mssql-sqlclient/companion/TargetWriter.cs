using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace Dpone.Mssql.SqlClient;

internal sealed record WriteTimings(double LaunchSeconds, double WriteSeconds, double DisposeSeconds, long Rows);

internal sealed class TargetWriteException(string code, Exception innerException) : Exception(code, innerException)
{
    internal string Code { get; } = code;
}

internal static class TargetWriter
{
    internal static async Task<WriteTimings> WriteAsync(
        WriteRequest request, SqlCredentials credentials, Stopwatch deadline, CancellationToken cancellationToken)
    {
        Stopwatch launch = Stopwatch.StartNew();
        SqlConnectionStringBuilder builder = new()
        {
            DataSource = $"{credentials.Host},{credentials.Port}",
            InitialCatalog = credentials.Database,
            UserID = credentials.Username,
            Password = credentials.Password,
            Encrypt = credentials.Encrypt,
            TrustServerCertificate = credentials.TrustServerCertificate,
            Pooling = false,
            ConnectTimeout = RemainingSeconds(request, deadline),
            ApplicationName = Protocol.ApplicationName(request),
        };
        await using SqlConnection connection = new(builder.ConnectionString);
        builder.Password = string.Empty;
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException and not TimeoutException)
        {
            string code = exception is SqlException sqlException
                ? $"mssql_sqlclient.connection_failed_sql_{sqlException.Number}"
                : exception is PlatformNotSupportedException
                    ? "mssql_sqlclient.connection_platform_unsupported"
                    : exception is NotSupportedException
                        ? "mssql_sqlclient.connection_runtime_unsupported"
                    : exception is FileNotFoundException
                        ? "mssql_sqlclient.connection_dependency_missing"
                        : exception is TypeInitializationException
                            ? "mssql_sqlclient.connection_runtime_initialization_failed"
                            : exception is ArgumentException
                                ? "mssql_sqlclient.connection_configuration_invalid"
                                : exception is InvalidOperationException
                                    ? "mssql_sqlclient.connection_state_invalid"
                                    : "mssql_sqlclient.connection_failed";
            throw new TargetWriteException(code, exception);
        }
        try
        {
            await AcquireLockAsync(connection, request, deadline, cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException and not TimeoutException)
        {
            throw new TargetWriteException("mssql_sqlclient.applock_failed", exception);
        }
        try
        {
            await ValidateTargetAsync(connection, request, deadline, cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException and not TimeoutException)
        {
            throw new TargetWriteException("mssql_sqlclient.target_validation_failed", exception);
        }
        launch.Stop();

        Stopwatch write = Stopwatch.StartNew();
        long rows;
        try
        {
            using NativeFileDataReader reader = new(request);
            using SqlBulkCopy bulk = new(connection,
                SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction, null);
            bulk.DestinationTableName = request.QualifiedStage;
            bulk.EnableStreaming = true;
            bulk.BatchSize = 0;
            bulk.BulkCopyTimeout = RemainingSeconds(request, deadline);
            foreach (ColumnSpec column in request.Columns)
                bulk.ColumnMappings.Add(column.TargetName, column.TargetName);
            if (request.LayoutVersion == 2)
                bulk.ColumnMappings.Add("__dpone__native_row_hash", "__dpone__native_row_hash");
            await bulk.WriteToServerAsync(reader, cancellationToken);
            rows = reader.RowsRead;
            if (rows != request.ExpectedRows)
                throw new InvalidDataException();
        }
        catch (Exception exception) when (exception is not OperationCanceledException and not TimeoutException)
        {
            throw new TargetWriteException("mssql_sqlclient.bulk_copy_failed", exception);
        }
        write.Stop();

        Stopwatch dispose = Stopwatch.StartNew();
        await connection.DisposeAsync();
        dispose.Stop();
        return new WriteTimings(launch.Elapsed.TotalSeconds, write.Elapsed.TotalSeconds, dispose.Elapsed.TotalSeconds, rows);
    }

    private static async Task AcquireLockAsync(
        SqlConnection connection, WriteRequest request, Stopwatch deadline, CancellationToken cancellationToken)
    {
        await using SqlCommand command = connection.CreateCommand();
        command.CommandText = "DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=@resource, @LockMode=N'Exclusive', " +
            "@LockOwner=N'Session', @LockTimeout=@timeout; SELECT @r;";
        command.CommandTimeout = RemainingSeconds(request, deadline);
        command.Parameters.Add("@resource", SqlDbType.NVarChar, 255).Value = Protocol.ApplicationLock(request.GrantTokenSha256);
        command.Parameters.Add("@timeout", SqlDbType.Int).Value = RemainingMilliseconds(request, deadline);
        object? raw = await command.ExecuteScalarAsync(cancellationToken);
        if (raw is not int result || result < 0)
            throw new InvalidOperationException();
    }

    private static async Task ValidateTargetAsync(
        SqlConnection connection, WriteRequest request, Stopwatch deadline, CancellationToken cancellationToken)
    {
        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = "SELECT OBJECT_ID(@stage,N'U'), CONVERT(nvarchar(128),ep.value) " +
                "FROM sys.extended_properties ep WHERE ep.class=1 AND ep.major_id=OBJECT_ID(@stage,N'U') " +
                "AND ep.minor_id=0 AND ep.name=N'dpone_native_owner';";
            command.CommandTimeout = RemainingSeconds(request, deadline);
            command.Parameters.Add("@stage", SqlDbType.NVarChar, 776).Value = request.QualifiedStage;
            await using SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.GetInt32(0) != request.ObjectId ||
                !string.Equals(reader.GetString(1), request.OwnerBindingSha256, StringComparison.Ordinal))
                throw new InvalidOperationException();
            if (await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException();
        }
        await using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandText = "SELECT c.column_id-1,c.name,t.name,c.max_length,c.precision,c.scale,c.is_nullable " +
                "FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id " +
                "WHERE c.object_id=@object_id ORDER BY c.column_id;";
            command.CommandTimeout = RemainingSeconds(request, deadline);
            command.Parameters.Add("@object_id", SqlDbType.Int).Value = request.ObjectId;
            await using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
            int ordinal = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                if (ordinal < request.Columns.Count)
                {
                    ColumnSpec expected = request.Columns[ordinal];
                    if (reader.GetInt32(0) != ordinal ||
                        !string.Equals(reader.GetString(1), expected.TargetName, StringComparison.Ordinal) ||
                        !MatchesType(expected, reader.GetString(2), reader.GetInt16(3), reader.GetByte(4), reader.GetByte(5)) ||
                        reader.GetBoolean(6) != expected.Nullable)
                        throw new InvalidOperationException();
                }
                else if (request.LayoutVersion != 2 || !MatchesTechnicalColumn(reader, ordinal, request.Columns.Count))
                    throw new InvalidOperationException();
                ordinal++;
            }
            int expectedCount = request.Columns.Count + (request.LayoutVersion == 2 ? 2 : 0);
            if (ordinal != expectedCount) throw new InvalidOperationException();
        }
    }

    private static bool MatchesTechnicalColumn(SqlDataReader reader, int ordinal, int businessCount) =>
        reader.GetInt32(0) == ordinal && !reader.GetBoolean(6) &&
        (ordinal == businessCount
            ? reader.GetString(1) == "__dpone__native_row_hash" && reader.GetString(2) == "binary" && reader.GetInt16(3) == 32
            : ordinal == businessCount + 1 && reader.GetString(1) == "__dpone__mutation_version" &&
              reader.GetString(2) is "timestamp" or "rowversion" && reader.GetInt16(3) == 8);

    private static bool MatchesType(ColumnSpec expected, string type, short maxLength, byte precision, byte scale) =>
        expected.TargetType switch
        {
            "bigint" => type == "bigint" && maxLength == 8,
            "float(53)" => type == "float" && maxLength == 8 && precision == 53,
            "nvarchar(max)" => type == "nvarchar" && maxLength == -1,
            "datetime2(6)" => type == "datetime2" && maxLength == 8 && scale == 6,
            _ => false,
        };

    private static int RemainingSeconds(WriteRequest request, Stopwatch deadline) =>
        Math.Max(1, checked((int)Math.Ceiling(RemainingMilliseconds(request, deadline) / 1000.0)));

    private static int RemainingMilliseconds(WriteRequest request, Stopwatch deadline)
    {
        long remaining = request.DeadlineBudgetMs - deadline.ElapsedMilliseconds;
        if (remaining <= 0) throw new TimeoutException();
        return checked((int)Math.Min(int.MaxValue, remaining));
    }

}
