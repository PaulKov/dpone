using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Input;
using Microsoft.Data.SqlClient;

internal static class LiveSqlChecks
{
    internal static async Task Run(string root)
    {
        using var credentials = JsonDocument.Parse(await Console.In.ReadToEndAsync());
        JsonElement cfg = credentials.RootElement;
        await using (var observer = Connection(cfg))
        {
            await observer.OpenAsync();
            var environment = new {
                runtime=System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                architecture=System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
                sqlVersion=await Scalar(observer,"SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion'))"),
                sqlMaxMemoryMiB=await Scalar(observer,"SELECT CONVERT(bigint,value_in_use) FROM sys.configurations WHERE name='max server memory (MB)'"),
                logBytes=await Scalar(observer,"SELECT SUM(CONVERT(bigint,size))*8192 FROM sys.database_files WHERE type=1"),
                sdkAssembly=typeof(SqlConnection).Assembly.GetName().Version?.ToString(),
                arrowAssembly=typeof(Apache.Arrow.RecordBatch).Assembly.GetName().Version?.ToString(),
                inputBudgetBytes=67108864, batchRows=3, operationTimeoutSeconds=30,
                profile="component-only; no guarded-exec or durable grant certification"
            };
            File.WriteAllText(Path.Combine(root,"live-environment.json"),JsonSerializer.Serialize(environment,new JsonSerializerOptions{WriteIndented=true}));
        }
        var records = new List<object>();
        foreach (string mode in new[] { "rows", "arrow" })
        foreach (var item in LiveCases.Read(root))
            records.Add(await Execute(cfg, item, mode));
        File.WriteAllText(Path.Combine(root,"live-results.json"), JsonSerializer.Serialize(records,new JsonSerializerOptions{WriteIndented=true}));
        bool pass = records.All(r => JsonSerializer.SerializeToElement(r).GetProperty("status").GetString() == "PASS");
        Console.WriteLine(JsonSerializer.Serialize(new { status=pass?"PASS":"FAIL", cases=records.Count }));
        if (!pass) throw new InvalidDataException("live.failed_cases");
    }
    private static SqlConnection Connection(JsonElement cfg, string? user = null, string? password = null)
    {
        var b = new SqlConnectionStringBuilder { DataSource=cfg.GetProperty("server").GetString(), InitialCatalog=cfg.GetProperty("database").GetString(),
            UserID=user ?? cfg.GetProperty("user").GetString(), Password=password ?? cfg.GetProperty("password").GetString(),
            Pooling=false, Enlist=false, MultipleActiveResultSets=false, ConnectRetryCount=0, TrustServerCertificate=true, ConnectTimeout=15 };
        b["Encrypt"]="true";
        return new SqlConnection(b.ConnectionString);
    }
    private static async Task<object> Execute(JsonElement cfg, LiveCase item, string mode)
    {
        string id=Guid.NewGuid().ToString("N"); string schema="dda_bulk_"+id; string login="dda_writer_"+id;
        string target=LiveCases.Quote(schema)+".[stage]", audit=LiveCases.Quote(schema)+".[audit]";
        string password=Convert.ToBase64String(RandomNumberGenerator.GetBytes(36)); byte[] nonce=RandomNumberGenerator.GetBytes(16);
        bool createdSchema=false, createdLogin=false, createdUser=false, cleaned=false;
        string status="FAIL", code="not_started"; long observedRows=-1; int retained=0; BulkCopyOutcome? outcome=null;
        bool identityStable=false, auditBound=false, workerOpened=false; string? errorType=null;
        var budget=new InputBufferBudget(67108864); var policy=new BulkInputPolicy(3,budget.LimitBytes);
        await using var admin=Connection(cfg); await admin.OpenAsync();
        SqlConnection? writer=null;
        try
        {
            await Exec(admin,$"CREATE SCHEMA [{schema}]"); createdSchema=true;
            string definitions=string.Join(',',item.Contract.Columns.Select(c=>LiveCases.Quote(c.Name)+" "+c.DataTypeName+(c.Nullable?" NULL":" NOT NULL")));
            await Exec(admin,$"CREATE TABLE {target} ({definitions})");
            await Exec(admin,$"CREATE TABLE {audit}(rows_count bigint NOT NULL,spid int NOT NULL,nonce varbinary(128) NOT NULL)");
            await Exec(admin,$"CREATE TRIGGER [{schema}].[capture] ON {target} AFTER INSERT AS BEGIN SET NOCOUNT ON;INSERT INTO {audit} VALUES((SELECT COUNT_BIG(*) FROM inserted),@@SPID,CONTEXT_INFO());END");
            using var stream=File.OpenRead(item.File);
            using DbDataReader reader=mode=="rows" ? new NativeInputReader(stream,item.Contract,default,budget) : new BoundedArrowReader(stream,item.Contract,policy,budget);
            InputReceipt Complete()=>reader is NativeInputReader native?native.RequireComplete():((BoundedArrowReader)reader).RequireComplete();
            if(item.Contract.Expected.Rows==0)
            {
                if(reader.Read() || Complete().Rows!=0) throw new InvalidDataException("live.empty_input");
                outcome=new BulkCopyOutcome("empty_input_only",Complete());
                observedRows=Convert.ToInt64(await Scalar(admin,$"SELECT COUNT_BIG(*) FROM {target}"));
                status=observedRows==0?"PASS":"FAIL"; code="empty_branch_no_worker_connection";
            }
            else
            {
                await Exec(admin,$"CREATE LOGIN [{login}] WITH PASSWORD='{password}',CHECK_POLICY=OFF"); createdLogin=true;
                await Exec(admin,$"CREATE USER [{login}] FOR LOGIN [{login}]"); createdUser=true;
                await Exec(admin,$"GRANT SELECT,INSERT ON {target} TO [{login}]");
                writer=Connection(cfg,login,password); await writer.OpenAsync(); workerOpened=true;
                await using(var mark=new SqlCommand("SET CONTEXT_INFO @nonce",writer)) {mark.Parameters.Add("@nonce",SqlDbType.VarBinary,128).Value=nonce;await mark.ExecuteNonQueryAsync();}
                var before=await Identity(admin,login,nonce);
                var copy=new ExistingConnectionBulkWriter(writer,reader,item.Contract,new BulkDestination(cfg.GetProperty("database").GetString()!,schema,"stage"),policy,
                    new BulkDeadline(TimeProvider.System,Stopwatch.GetTimestamp()+Stopwatch.Frequency*30),Complete);
                outcome=await copy.WriteAsync(value=>{retained++;outcome=value;});
                var after=await Identity(admin,login,nonce); identityStable=before==after;
                observedRows=Convert.ToInt64(await Scalar(admin,$"SELECT COUNT_BIG(*) FROM {target}"));
                await using(var bound=new SqlCommand($"SELECT COUNT_BIG(*) FROM {audit} WHERE spid<>@spid OR nonce<>@nonce",admin))
                {
                    bound.Parameters.AddWithValue("@spid",before.Spid);bound.Parameters.Add("@nonce",SqlDbType.VarBinary,128).Value=nonce;
                    auditBound=Convert.ToInt64(await bound.ExecuteScalarAsync())==0 && Convert.ToInt64(await Scalar(admin,$"SELECT COALESCE(SUM(rows_count),0) FROM {audit}"))==observedRows;
                }
                bool parity=item.Malformed ? observedRows==3 && outcome.Code=="copy_failed" && outcome.Input is null :
                    outcome.Code=="copied_input_complete" && await Parity(admin,target,item);
                status=parity&&identityStable&&auditBound&&retained==1?"PASS":"FAIL";
                code=item.Malformed?"malformed_after_committed_batch":"exact_typed_copy";
            }
        }
        catch(Exception error){errorType=error.GetType().Name;code="live_component_exception";}
        finally
        {
            if(writer is not null) await writer.DisposeAsync();
            bool absent=false;
            for(int i=0;i<50;i++)
            {
                await using var sessions=new SqlCommand("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE original_login_name=@login",admin);
                sessions.Parameters.AddWithValue("@login",login);
                if(Convert.ToInt32(await sessions.ExecuteScalarAsync())==0){absent=true;break;}
                await Task.Delay(50);
            }
            if(absent)
            {
                if(createdSchema){await Exec(admin,$"DROP TABLE IF EXISTS {target}");await Exec(admin,$"DROP TABLE IF EXISTS {audit}");}
                if(createdUser)await Exec(admin,$"DROP USER [{login}]");
                if(createdSchema)await Exec(admin,$"DROP SCHEMA [{schema}]");
                if(createdLogin)await Exec(admin,$"DROP LOGIN [{login}]");
                cleaned=true;
            }
            if(!cleaned || budget.UsedBytes!=0)status="FAIL";
        }
        return new{item.Name,mode,status,code,outcome,observedRows,retained,identityStable,auditBound,workerOpened,cleaned,budgetBytes=budget.UsedBytes,errorType};
    }
    private static async Task<(int Spid,Guid Connection,DateTime Login)> Identity(SqlConnection admin,string login,byte[] nonce)
    {
        await using var command=new SqlCommand("SELECT s.session_id,c.connection_id,s.login_time FROM sys.dm_exec_sessions s JOIN sys.dm_exec_connections c ON c.session_id=s.session_id WHERE s.original_login_name=@login AND s.context_info=@nonce",admin);
        command.Parameters.AddWithValue("@login",login);command.Parameters.Add("@nonce",SqlDbType.VarBinary,128).Value=nonce;
        await using var rows=await command.ExecuteReaderAsync();
        if(!await rows.ReadAsync())throw new InvalidDataException("live.writer_identity");
        var value=((int)rows.GetInt16(0),rows.GetGuid(1),rows.GetDateTime(2));
        if(await rows.ReadAsync())throw new InvalidDataException("live.writer_ambiguous");return value;
    }
    private static async Task<bool> Parity(SqlConnection admin,string target,LiveCase item)
    {
        var rows=new List<string>();await using var command=new SqlCommand("SELECT "+string.Join(',',item.Contract.Columns.Select(c=>LiveCases.Quote(c.Name)))+" FROM "+target,admin);
        await using var reader=await command.ExecuteReaderAsync();
        while(await reader.ReadAsync())rows.Add(JsonSerializer.Serialize(item.Contract.Columns.Select((c,i)=>reader.IsDBNull(i)?null:LiveCases.Canonical(reader.GetValue(i),c)).ToArray()));
        return rows.Order(StringComparer.Ordinal).SequenceEqual(item.Expected!.Select(r=>JsonSerializer.Serialize(r)).Order(StringComparer.Ordinal));
    }
    private static async Task Exec(SqlConnection connection,string sql){await using var command=new SqlCommand(sql,connection){CommandTimeout=15};await command.ExecuteNonQueryAsync();}
    private static async Task<object?> Scalar(SqlConnection connection,string sql){await using var command=new SqlCommand(sql,connection){CommandTimeout=15};return await command.ExecuteScalarAsync();}
}
