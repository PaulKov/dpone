using System.Text.Json;
using System.Text.RegularExpressions;

namespace Dpone.SqlClient.Startup;

/// <summary>Fixed trusted composition roots; manifest digest authority comes from the launch.</summary>
public sealed record StartupInstallation(string CompanionRoot, string RuntimeRoot, string ManifestPath);
internal sealed record DeploymentFile(string Origin, string Path, string Role, string Sha256);
internal sealed class DeploymentManifest
{
    internal IReadOnlyList<DeploymentFile> Files { get; }
    internal DeploymentFile Role(string role) => Files.Single(file => file.Role == role);
    internal DeploymentManifest(byte[] body, string expected)
    {
        if (body.Length is < 1 or > 2097152) throw new InvalidDataException("startup.manifest_size");
        using JsonDocument document = JsonWire.Document(body);
        JsonElement value = document.RootElement;
        JsonWire.Shape(value, "schema_version", "backend", "platform", "runtime_version", "sqlclient_version", "arrow_version", "files");
        JsonWire.Integer(value, "schema_version", 1, 1);
        foreach ((string field, string wanted) in new[] { ("backend", "mssql_sqlclient"), ("platform", "linux_arm64"), ("runtime_version", "8.0.31"), ("sqlclient_version", "7.0.2"), ("arrow_version", "23.0.0") })
            if (JsonWire.Text(value, field) != wanted) throw new InvalidDataException("startup.manifest_profile");
        JsonElement entries = value.GetProperty("files");
        if (entries.ValueKind != JsonValueKind.Array || entries.GetArrayLength() is < 7 or > 4096)
            throw new InvalidDataException("startup.manifest_count");
        var files = new List<DeploymentFile>();
        string? previous = null;
        foreach (JsonElement row in entries.EnumerateArray())
        {
            JsonWire.Shape(row, "origin", "path", "role", "sha256");
            string origin = JsonWire.Text(row, "origin"), path = JsonWire.Text(row, "path"), role = JsonWire.Text(row, "role");
            Relative(path);
            if (origin is not ("companion" or "dotnet") || role is not ("dotnet_host" or "runtime_library" or "worker_assembly" or "worker_runtimeconfig" or "worker_deps" or "managed_dependency" or "companion_native") ||
                (origin == "dotnet") != (role is "dotnet_host" or "runtime_library")) throw new InvalidDataException("startup.manifest_role");
            string key = origin + "\0" + path;
            if (previous is not null && string.CompareOrdinal(previous, key) >= 0) throw new InvalidDataException("startup.manifest_order");
            previous = key;
            files.Add(new(origin, path, role, JsonWire.Hash(row, "sha256")));
        }
        foreach (string role in new[] { "dotnet_host", "worker_assembly", "worker_runtimeconfig", "worker_deps" })
            if (files.Count(file => file.Role == role) != 1) throw new InvalidDataException("startup.manifest_singleton");
        foreach (string name in new[] { "Microsoft.Data.SqlClient.dll", "Apache.Arrow.dll" })
            if (files.Count(file => file.Role == "managed_dependency" && System.IO.Path.GetFileName(file.Path) == name) != 1)
                throw new InvalidDataException("startup.manifest_dependency");
        if (!files.Any(file => file.Role == "runtime_library") || JsonWire.Digest("dpone.sqlclient.deployment.v1\0", value) != expected)
            throw new InvalidDataException("startup.manifest_binding");
        Files = files.AsReadOnly();
    }
    internal static void Relative(string path)
    {
        if (path.Length is < 1 or > 1024 || !Regex.IsMatch(path, @"\A[A-Za-z0-9_.+-]+(?:/[A-Za-z0-9_.+-]+)*\z") || path.Split('/').Any(part => part is "." or ".."))
            throw new InvalidDataException("startup.manifest_path");
    }
}
