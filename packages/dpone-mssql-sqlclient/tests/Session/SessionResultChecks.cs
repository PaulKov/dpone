using System.Text;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Input;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;

internal static class SessionResultChecks
{
    internal static void Run(string fixtures)
    {
        var launch=StartupLaunch.Parse(File.ReadAllBytes(Path.Combine(fixtures,"launch-v1.json")));
        var input=new InputReceipt(1,8,new string('a',64));
        var empty=new InputReceipt(0,0,"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        const string grant="44444444-4444-4444-8444-444444444444";
        foreach(string name in new[]{"empty","granted-error","pregrant-error","success"})
        {
            byte[] bytes=File.ReadAllBytes(Path.Combine(fixtures,"result-"+name+"-v1.json"));
            var decoded=SessionResult.Decode(bytes,launch,name=="empty"?empty:input,name=="empty"?null:grant);
            Check(decoded.Encode().SequenceEqual(bytes.AsSpan().TrimEnd((byte)'\n').ToArray()),"golden_result_bytes");
        }
        byte[] success=File.ReadAllBytes(Path.Combine(fixtures,"result-success-v1.json"));
        byte[] failure=File.ReadAllBytes(Path.Combine(fixtures,"result-pregrant-error-v1.json"));
        SessionResult.Decode(failure,launch,input,null);
        var maximum=new InputReceipt(long.MaxValue,long.MaxValue,new string('a',64));
        var maximumResult=new SessionResult(launch.Digest,launch.AttemptSha256,grant,maximum,null);
        Check(SessionResult.Decode(maximumResult.Encode(),launch,maximum,grant).Receipt==maximum,"int64_receipt");
        Reject(()=>new SessionResult(launch.Digest,launch.AttemptSha256,null,input,null));
        Reject(()=>new SessionResult(launch.Digest,launch.AttemptSha256,grant,empty,null));
        Reject(()=>new SessionResult(launch.Digest,launch.AttemptSha256,grant,input,SessionResultError.Driver));
        Reject(()=>new SessionResult(launch.Digest,launch.AttemptSha256,null,null,null));
        Reject(()=>new SessionResult(launch.Digest,launch.AttemptSha256,null,null,(SessionResultError)999));
        Reject(()=>SessionResult.Decode(success,launch,input,null));
        Reject(()=>SessionResult.Decode(success,launch,input,"55555555-5555-4555-8555-555555555555"));
        Reject(()=>SessionResult.Decode(success,launch,new InputReceipt(2,8,input.Sha256),grant));
        Reject(()=>SessionResult.Decode(success,launch,new InputReceipt(1,9,input.Sha256),grant));
        Reject(()=>SessionResult.Decode(success,launch,new InputReceipt(1,8,new string('b',64)),grant));
        foreach(var malformed in new[]{new InputReceipt(0,8,input.Sha256),new InputReceipt(1,0,input.Sha256),new InputReceipt(0,0,input.Sha256),new InputReceipt(-1,8,input.Sha256)})
            Reject(()=>SessionResult.Decode(failure,launch,malformed,grant));
        Reject(()=>SessionResult.Decode(failure,launch,empty,grant));
        Reject(()=>SessionResult.Decode(failure,launch,input,Guid.Empty.ToString("D")));
        foreach(string path in new[]{"launch_sha256","attempt_sha256","result.attempt_sha256"})
            Reject(()=>SessionResult.Decode(Change(success,path,JsonValue.Create(new string('b',64))),launch,input,grant));
        foreach(string path in new[]{"schema_version","result.schema_version","result.input_eof"})
            Reject(()=>SessionResult.Decode(Change(success,path,JsonNode.Parse("1.0")),launch,input,grant));
        foreach(string path in new[]{"schema_version","launch_sha256","attempt_sha256","result","result.status","result.attempt_sha256","result.input_eof"})
            Reject(()=>SessionResult.Decode(Change(success,path,null),launch,input,grant));
        Reject(()=>SessionResult.Decode(Change(success,"result.error",JsonValue.Create("driver")),launch,input,grant));
        Reject(()=>SessionResult.Decode(Change(success,"result.status",JsonValue.Create("error")),launch,input,grant));
        Reject(()=>SessionResult.Decode(Change(success,"result.input_eof",JsonValue.Create(false)),launch,input,grant));
        Reject(()=>SessionResult.Decode(Change(failure,"result.error",null),launch,input,grant));
        foreach(string bad in new[]{"arbitrary diagnostic","DRIVER","", "0"})
            Reject(()=>SessionResult.Decode(Change(failure,"result.error",JsonValue.Create(bad)),launch,input,grant));
        foreach(string code in new[]{"startup_timeout","operation_timeout","resource_limit","decoder","driver","constraint","permission","connection","process_unknown","ownership","fencing","verification","protocol","cleanup","coordinator_lost"})
        {
            byte[] body=Change(failure,"result.error",JsonValue.Create(code));
            var result=SessionResult.Decode(body,launch,input,grant);
            Check(JsonNode.Parse(result.Encode())!["result"]!["error"]!.GetValue<string>()==code,"error_vocabulary");
        }
        foreach(string body in new[]{"{}",Encoding.UTF8.GetString(success).Replace("\"schema_version\":1","\"schema_version\":1,\"schema_version\":1"),Encoding.UTF8.GetString(success).Replace("\"schema_version\":1","\"schema_version\":1,\"extra\":0")})
            Reject(()=>SessionResult.Decode(Encoding.UTF8.GetBytes(body),launch,input,grant));
        Reject(()=>SessionResult.Decode(new byte[]{255},launch,input,grant));
        Reject(()=>SessionResult.Decode(new byte[16385],launch,input,grant));
        Console.WriteLine("PASS four result golden bodies, all15 errors, original bindings, grant/input consistency and strict malformed controls");
    }
    private static byte[] Change(byte[] bytes,string path,JsonNode? value)
    {
        var root=JsonNode.Parse(bytes)!;var parts=path.Split('.');JsonNode node=root;
        foreach(string part in parts[..^1])node=node[part]!;node[parts[^1]]=value;
        return Encoding.UTF8.GetBytes(root.ToJsonString());
    }
    private static void Check(bool value,string code){if(!value)throw new Exception(code);}
    private static void Reject(Action action){try{action();}catch(InvalidDataException){return;}throw new Exception("expected_result_rejection");}
}
