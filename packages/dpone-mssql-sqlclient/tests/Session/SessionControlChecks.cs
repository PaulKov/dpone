using System.Text;
using System.Text.Json.Nodes;
using Dpone.SqlClient.Session;
using Dpone.SqlClient.Startup;

internal static class SessionControlChecks
{
    internal static void Run(string fixtures, string? unicodeVectors = null)
    {
        byte[] grantBytes = File.ReadAllBytes(Path.Combine(fixtures,"bulk-grant-principal-v1.json"));
        var grant = SessionControlCodec.DecodeGrant(grantBytes);
        var launch = StartupLaunch.Parse(File.ReadAllBytes(Path.Combine(fixtures,"launch-v1.json")));
        var ownership = new SessionOwnership("владелец 🧪", long.MaxValue, "33333333-3333-4333-8333-333333333333");
        var stage = new SessionObjectIdentity(123, new string('e',64));
        var remote = new RemoteSessionIdentity("11111111-1111-1111-1111-111111111111",72,"2026-01-02T03:04:05.006000",
            "2026-01-02T03:04:05.009000",string.Concat(Enumerable.Repeat("6e",31))+"00",string.Concat(Enumerable.Repeat("61",32)));
        const string evidence = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        grant.Validate(launch, ownership, stage, remote, evidence, Principal(), 0);
        grant.Validate(launch, ownership, stage, remote, evidence, Principal(), launch.OperationDeadlineNs-1);
        Reject(()=>SessionControlCodec.DecodeGrant(File.ReadAllBytes(Path.Combine(fixtures,"bulk-grant-v1.json"))));
        BindingChanges(grantBytes, launch, ownership, stage, remote, evidence);
        StructuralChanges(grantBytes);
        byte[] announcementBytes = File.ReadAllBytes(Path.Combine(fixtures,"session-announcement-v1.json"));
        var announcement = SessionControlCodec.DecodeAnnouncement(announcementBytes);
        Check(SessionControlCodec.EncodeAnnouncement(announcement).SequenceEqual(announcementBytes.AsSpan().TrimEnd((byte)'\n').ToArray()),"announcement_bytes");
        announcement.Validate(launch,Convert.FromHexString(announcement.Nonce),0);
        foreach (string path in new[] {"schema_version","grant_id","launch_sha256","attempt_sha256","ownership.owner","ownership.fence","ownership.supervisor_id",
            "process.host_sha256","process.boot_id","process.pid","process.start_ticks","object_identity.object_id","object_identity.fingerprint",
            "input_binding_sha256","build_sha256","remote_session.schema","remote_session.connection_id","remote_session.session_id","remote_session.connect_time",
            "remote_session.login_time","remote_session.nonce","remote_session.authority_sha256","writer_observation_sha256","operation_deadline_ns","resolved_database_principal.principal_id","resolved_database_principal.name","resolved_database_principal.sid"})
        {
            var node=JsonNode.Parse(grantBytes)!; var parts=path.Split('.'); JsonNode parent=node;
            foreach(string part in parts[..^1]) parent=parent[part]!;
            parent[parts[^1]]=null;
            Reject(()=>SessionControlCodec.DecodeGrant(Encoding.UTF8.GetBytes(node.ToJsonString())));
        }
        Reject(()=>grant.Validate(launch,grant.Ownership,grant.ObjectIdentity,grant.RemoteSession,grant.WriterObservationSha256,Principal(),launch.OperationDeadlineNs));
        Reject(()=>grant.Validate(launch,grant.Ownership,grant.ObjectIdentity,grant.RemoteSession,new string('0',64),Principal(),0));
        Reject(()=>announcement.Validate(launch,new byte[32],0));
        Reject(()=>SessionControlCodec.DecodeGrant(new byte[16385]));
        Reject(()=>SessionControlCodec.DecodeGrant(new byte[]{255}));
        var boundary=new SessionOwnership(string.Concat(Enumerable.Repeat("🧪",256)),long.MaxValue,"33333333-3333-4333-8333-333333333333");
        Check(boundary.Owner.Length==512,"unicode_scalar_count");
        Reject(()=>new SessionOwnership(boundary.Owner+"x",1,boundary.SupervisorId));
        Reject(()=>new SessionOwnership("\ud800",1,boundary.SupervisorId));
        Reject(()=>grant.Validate(launch,ownership,stage,remote,evidence,Principal(),-1));
        Reject(()=>announcement.Validate(launch,Convert.FromHexString(announcement.Nonce),launch.OperationDeadlineNs));
        Reject(()=>SessionControlCodec.DecodeAnnouncement(Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(announcementBytes).Replace("\"session_id\":72","\"session_id\":true"))));
        Reject(()=>SessionControlCodec.DecodeAnnouncement(Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(announcementBytes).Replace(announcement.Nonce,new string('0',64)))));
        if (!SessionControlCodec.EncodeGrant(grant).SequenceEqual(grantBytes.AsSpan().TrimEnd((byte)'\n').ToArray())) throw new Exception("canonical_unicode_grant_bytes");
        Check(SessionControlCodec.GrantDigest(grant) == File.ReadAllText(Path.Combine(fixtures,"bulk-grant-principal-v1.sha256")).Trim(),"grant_digest");
        if(unicodeVectors is not null)
        {
            using var vectors=System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(unicodeVectors));
            foreach(var vector in vectors.RootElement.EnumerateArray())
            {
                byte[] expected=Convert.FromBase64String(vector.GetProperty("body_base64").GetString()!);
                var parsed=SessionControlCodec.DecodeGrant(expected);
                Check(SessionControlCodec.EncodeGrant(parsed).SequenceEqual(expected),"unicode_differential_bytes");
                Check(SessionControlCodec.GrantDigest(parsed)==vector.GetProperty("sha256").GetString(),"unicode_differential_digest");
            }
        }
        Console.WriteLine("PASS session announcement/grant golden bytes, digest, all original bindings, strict fields, deadlines and Unicode boundaries");
    }
    private static byte[] Changed(byte[] source,string path,JsonNode? replacement)
    {
        var node=JsonNode.Parse(source)!;var parts=path.Split('.');JsonNode parent=node;
        foreach(string part in parts[..^1])parent=parent[part]!;
        parent[parts[^1]]=replacement;
        return Encoding.UTF8.GetBytes(node.ToJsonString());
    }
    private static void BindingChanges(byte[] body,StartupLaunch launch,SessionOwnership ownership,SessionObjectIdentity stage,RemoteSessionIdentity remote,string evidence)
    {
        var changes=new (string Path,JsonNode Value)[] {
            ("launch_sha256",JsonValue.Create(new string('0',64))!), ("attempt_sha256",JsonValue.Create(new string('0',64))!),
            ("ownership.owner",JsonValue.Create("other")!), ("ownership.fence",JsonValue.Create(1)!),
            ("ownership.supervisor_id",JsonValue.Create("55555555-5555-4555-8555-555555555555")!),
            ("process.host_sha256",JsonValue.Create(new string('0',64))!), ("process.boot_id",JsonValue.Create("55555555-5555-4555-8555-555555555555")!),
            ("process.pid",JsonValue.Create(124)!), ("process.start_ticks",JsonValue.Create(457)!),
            ("object_identity.object_id",JsonValue.Create(124)!), ("object_identity.fingerprint",JsonValue.Create(new string('0',64))!),
            ("input_binding_sha256",JsonValue.Create(new string('0',64))!), ("build_sha256",JsonValue.Create(new string('0',64))!),
            ("remote_session.connection_id",JsonValue.Create("55555555-5555-4555-8555-555555555555")!),
            ("remote_session.session_id",JsonValue.Create(73)!), ("remote_session.connect_time",JsonValue.Create("2026-01-02T03:04:05.007000")!),
            ("remote_session.login_time",JsonValue.Create("2026-01-02T03:04:05.010000")!), ("remote_session.nonce",JsonValue.Create(new string('1',64))!),
            ("remote_session.authority_sha256",JsonValue.Create(new string('0',64))!),
            ("writer_observation_sha256",JsonValue.Create(new string('0',64))!), ("operation_deadline_ns",JsonValue.Create(20000000001L)!),
            ("resolved_database_principal.principal_id",JsonValue.Create(8)!),
            ("resolved_database_principal.name",JsonValue.Create("other")!),
            ("resolved_database_principal.sid",JsonValue.Create(new string('a',32))!) };
        foreach(var change in changes)
        {
            var changed=SessionControlCodec.DecodeGrant(Changed(body,change.Path,change.Value));
            Reject(()=>changed.Validate(launch,ownership,stage,remote,evidence,Principal(),0));
        }
    }
    private static void StructuralChanges(byte[] body)
    {
        foreach(string path in new[]{"grant_id","ownership.supervisor_id","process.boot_id","remote_session.connection_id"})
            Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,path,JsonValue.Create(Guid.Empty.ToString("D")))));
        foreach(string stamp in new[]{"1900-01-01T00:00:00.000000","2026-01-02T03:04:05.006","2026-01-02T03:04:05.006000Z","2026-02-30T03:04:05.006000"})
            Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,"remote_session.connect_time",JsonValue.Create(stamp))));
        foreach(string path in new[]{"ownership.fence","process.pid","object_identity.object_id","remote_session.session_id","operation_deadline_ns"})
            Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,path,JsonValue.Create(0))));
        Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,"remote_session.session_id",JsonValue.Create(32768))));
        foreach(string sid in new[]{"", "AB", "a", "aa ", new string('a',172)})
            Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,"resolved_database_principal.sid",JsonValue.Create(sid))));
        foreach(string number in new[]{"true", "1.0", "1e0", "0", "2147483648"})
            Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,"resolved_database_principal.principal_id",JsonNode.Parse(number))));
        Reject(()=>new ResolvedDatabasePrincipal(7,string.Concat(Enumerable.Repeat("🧪",65)),"ab"));
        Reject(()=>new ResolvedDatabasePrincipal(7,"bad\nname","ab"));
        Check(new ResolvedDatabasePrincipal(7,string.Concat(Enumerable.Repeat("🧪",64)),"ab").Name.Length==128,"principal_utf16_boundary");
        Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,"remote_session.nonce",JsonValue.Create(new string('0',64)))));
        Reject(()=>SessionControlCodec.DecodeGrant(Changed(body,"ownership.owner",JsonValue.Create("control\n"))));
        foreach(string bad in new[]{"\"schema_version\":true","\"schema_version\":1.0","\"schema_version\":1,\"schema_version\":1","\"schema_version\":1,\"unknown\":1"})
            Reject(()=>SessionControlCodec.DecodeGrant(Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(body).Replace("\"schema_version\":1",bad))));
    }
    private static ResolvedDatabasePrincipal Principal() => new(7,"writer",string.Concat(Enumerable.Repeat("cd",16)));
    private static void Check(bool condition,string code){if(!condition)throw new Exception(code);}
    private static void Reject(Action action){try{action();}catch(InvalidDataException){return;}throw new Exception("expected_session_rejection");}
}
