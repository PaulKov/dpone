using System.Security.Cryptography;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Input;

byte[] bytes = new byte[8];
var contract = new NativeInputContract(new[] { NativeColumn.Admit("value", "bigint", false, "bigint", 0, 8, null, null, null) },
    new ExpectedInput(1,8,Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant()),new NativeInputLimits(1024));
var policy = new BulkInputPolicy(1, 1 << 20);
var budget = new InputBufferBudget(policy.MaxInputBatchBytes);
using (var transport = budget.Reserve(65536))
{
    using var stream = new MemoryStream(bytes);
    using (var rows = new RowsInput(stream,contract,policy,budget,CancellationToken.None))
    {
        Check(ReferenceEquals(rows.Budget,budget),"same_ledger");
        Check(rows.Reader.Read() && rows.Reader.GetInt64(0)==0,"exact_value");
        Check(budget.UsedBytes>65536,"row_and_transport_overlap");
        Check(!rows.Reader.Read(),"natural_eof");
        Check(rows.Reader.RequireComplete().Rows==1,"receipt");
    }
    Check(stream.CanRead && budget.UsedBytes==65536,"caller_stream_and_transport_retained");
}
Check(budget.UsedBytes==0,"all_released");
using(var stream = new MemoryStream(bytes))
using(var held = budget.Reserve(budget.LimitBytes))
{
    Reject(()=>new RowsInput(stream,contract,policy,budget,CancellationToken.None));
    Check(stream.Position==0 && budget.UsedBytes==budget.LimitBytes,"constructor_denial_before_read");
}
using(var stream = new MemoryStream(bytes))
using(var held = budget.Reserve(budget.LimitBytes-1))
{
    using(var rows = new RowsInput(stream,contract,policy,budget,CancellationToken.None))
        Reject(()=>rows.Reader.Read());
    Check(budget.UsedBytes==budget.LimitBytes-1,"read_denial_releases_only_reader");
}
using(var stream = new MemoryStream(bytes))
{
    Reject(()=>new RowsInput(stream,contract,policy,new InputBufferBudget(2 << 20),CancellationToken.None));
    Check(stream.Position==0,"mismatch_before_read");
    using var legacy = new RowsInput(stream,contract,policy);
    Check(legacy.Reader.Read(),"original_constructor");
}
Console.WriteLine("PASS shared transport/rows ledger, overlap, denial, disposal, policy identity and original constructor");
static void Check(bool value,string code){if(!value)throw new Exception(code);}
static void Reject(Action call){try{call();}catch(InvalidDataException){return;}throw new Exception("expected_rejection");}
