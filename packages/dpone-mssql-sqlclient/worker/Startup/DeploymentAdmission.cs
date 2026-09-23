using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;

namespace Dpone.SqlClient.Startup;

internal static class DeploymentAdmission
{
    internal static void Verify(StartupInstallation installation, StartupLaunch launch, long deadline)
    {
        Root(installation.CompanionRoot); Root(installation.RuntimeRoot);
        var manifest = new DeploymentManifest(Read(installation.ManifestPath, 2097152, deadline), launch.BuildSha256);
        var companion = manifest.Files.Where(file => file.Origin == "companion").Select(file => file.Path).ToHashSet(StringComparer.Ordinal);
        if (!companion.SetEquals(Inventory(installation.CompanionRoot, deadline))) throw new InvalidDataException("startup.companion_inventory");
        string fxr = Path.Combine(installation.RuntimeRoot, "host/fxr");
        if (!Directory.GetFileSystemEntries(fxr).Select(Path.GetFileName).SequenceEqual(new[] { "8.0.31" }))
            throw new InvalidDataException("startup.runtime_inventory");
        var runtime = new HashSet<string>(StringComparer.Ordinal) { "dotnet" };
        foreach (string prefix in new[] { "host/fxr/8.0.31", "shared/Microsoft.NETCore.App/8.0.31" })
            foreach (string relative in Inventory(Path.Combine(installation.RuntimeRoot, prefix), deadline)) runtime.Add(prefix + "/" + relative);
        if (!runtime.Contains("host/fxr/8.0.31/libhostfxr.so") || !runtime.Contains("shared/Microsoft.NETCore.App/8.0.31/libhostpolicy.so") || !runtime.Contains("shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so") ||
            !runtime.SetEquals(manifest.Files.Where(file => file.Origin == "dotnet").Select(file => file.Path))) throw new InvalidDataException("startup.runtime_inventory");
        foreach (DeploymentFile file in manifest.Files)
        {
            string root = file.Origin == "companion" ? installation.CompanionRoot : installation.RuntimeRoot;
            if (Hash(Path.Combine(root, file.Path), deadline) != file.Sha256) throw new InvalidDataException("startup.file_changed");
        }
        string assembly = manifest.Role("worker_assembly").Path;
        string stem = assembly.EndsWith(".dll", StringComparison.Ordinal) ? assembly[..^4] : throw new InvalidDataException("startup.assembly");
        if (manifest.Role("worker_runtimeconfig").Path != stem + ".runtimeconfig.json" || manifest.Role("worker_deps").Path != stem + ".deps.json" ||
            companion.Any(path => path.EndsWith(".runtimeconfig.json", StringComparison.Ordinal) && path != stem + ".runtimeconfig.json" || path.EndsWith(".deps.json", StringComparison.Ordinal) && path != stem + ".deps.json"))
            throw new InvalidDataException("startup.configuration");
        Profile(Read(Path.Combine(installation.CompanionRoot, stem + ".runtimeconfig.json"), 65536, deadline));
        Dependencies(Read(Path.Combine(installation.CompanionRoot, stem + ".deps.json"), 2097152, deadline), companion);
        if (Assembly.GetEntryAssembly()?.Location != Path.Combine(installation.CompanionRoot, assembly) ||
            new FileInfo("/proc/self/exe").LinkTarget != Path.Combine(installation.RuntimeRoot, manifest.Role("dotnet_host").Path))
            throw new InvalidDataException("startup.loaded_origin");
        foreach (string name in new[] { "Microsoft.Data.SqlClient", "Apache.Arrow" })
        {
            DeploymentFile entry = manifest.Files.Single(file => file.Role == "managed_dependency" && Path.GetFileName(file.Path) == name + ".dll");
            string path = Path.Combine(installation.CompanionRoot, entry.Path);
            Assembly loaded = Assembly.LoadFrom(path);
            if (loaded.Location != path || loaded.GetName().Name != name || Hash(loaded.Location, deadline) != entry.Sha256)
                throw new InvalidDataException("startup.loaded_dependency");
        }
        foreach (string line in File.ReadLines("/proc/self/maps"))
        {
            int offset = line.IndexOf('/'); if (offset < 0) continue;
            string path = line[offset..];
            string? origin = path.StartsWith(installation.RuntimeRoot + "/", StringComparison.Ordinal) ? "dotnet" : path.StartsWith(installation.CompanionRoot + "/", StringComparison.Ordinal) ? "companion" : null;
            if (origin is null) continue; // OS platform admission is a separate deployment prerequisite.
            string root = origin == "dotnet" ? installation.RuntimeRoot : installation.CompanionRoot;
            if (!manifest.Files.Any(file => file.Origin == origin && Path.Combine(root, file.Path) == path))
                throw new InvalidDataException("startup.unlisted_loaded_file");
        }
        StartupClock.RequireBefore(deadline);
    }
    private static void Root(string path)
    {
        if (!Path.IsPathFullyQualified(path) || Path.GetFullPath(path) != path || !Directory.Exists(path)) throw new InvalidDataException("startup.root");
        NoLinks(path);
    }
    private static void NoLinks(string path)
    {
        for (string? current = Path.GetFullPath(path); current is not null; current = Path.GetDirectoryName(current))
            if (new FileInfo(current).LinkTarget is not null || new DirectoryInfo(current).LinkTarget is not null)
                throw new InvalidDataException("startup.symlink");
    }
    private static HashSet<string> Inventory(string root, long deadline)
    {
        Root(root);
        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (string path in Directory.EnumerateFileSystemEntries(root, "*", SearchOption.AllDirectories))
        {
            StartupClock.RequireBefore(deadline); NoLinks(path);
            if (result.Count >= 4096) throw new InvalidDataException("startup.inventory_size");
            if (Directory.Exists(path)) continue;
            string relative = Path.GetRelativePath(root, path); DeploymentManifest.Relative(relative);
            if (relative.EndsWith(".runtimeconfig.dev.json", StringComparison.Ordinal)) throw new InvalidDataException("startup.configuration");
            result.Add(relative);
        }
        return result;
    }
    private static byte[] Read(string path, int maximum, long deadline)
    {
        StartupClock.RequireBefore(deadline); NoLinks(path);
        using var handle = StartupFiles.Open(path);
        int fd = (int)handle.DangerousGetHandle();
        StartupFileIdentity before = StartupFiles.Identify(fd);
        using var stream = new FileStream(handle, FileAccess.Read);
        if (stream.Length > maximum) throw new InvalidDataException("startup.file_size");
        byte[] bytes = new byte[checked((int)stream.Length)]; int offset = 0;
        while (offset < bytes.Length)
        {
            StartupClock.RequireBefore(deadline);
            int n = stream.Read(bytes, offset, Math.Min(65536, bytes.Length - offset));
            if (n == 0) throw new InvalidDataException("startup.file_changed"); offset += n;
        }
        if (stream.ReadByte() != -1 || StartupFiles.Identify(fd) != before || StartupFiles.Identify(fd, path) != before) throw new InvalidDataException("startup.file_changed");
        StartupClock.RequireBefore(deadline); return bytes;
    }
    private static string Hash(string path, long deadline)
        => Convert.ToHexString(SHA256.HashData(Read(path, 268435456, deadline))).ToLowerInvariant();
    private static void Profile(byte[] bytes)
    {
        using JsonDocument doc = JsonWire.Document(bytes);
        JsonElement root = doc.RootElement; JsonWire.Shape(root, "runtimeOptions");
        JsonElement options = root.GetProperty("runtimeOptions");
        foreach (JsonProperty field in options.EnumerateObject())
            if (field.Name is not ("tfm" or "framework" or "rollForward" or "configProperties")) throw new InvalidDataException("startup.configuration");
        JsonElement framework = options.GetProperty("framework"); JsonWire.Shape(framework, "name", "version");
        JsonElement properties = options.GetProperty("configProperties");
        if (JsonWire.Text(framework, "name") != "Microsoft.NETCore.App" || JsonWire.Text(framework, "version") != "8.0.31" || JsonWire.Text(options, "rollForward") != "Disable" ||
            JsonWire.Integer(properties, "System.GC.HeapHardLimit", 536870912, 536870912) != 536870912 || properties.GetProperty("System.GC.Server").ValueKind != JsonValueKind.False)
            throw new InvalidDataException("startup.configuration");
        foreach (JsonProperty field in properties.EnumerateObject())
            if (field.Name.Contains("GC", StringComparison.Ordinal) && field.Name is not ("System.GC.HeapHardLimit" or "System.GC.Server")) throw new InvalidDataException("startup.configuration");
    }
    private static void Dependencies(byte[] bytes, HashSet<string> inventory)
    {
        using JsonDocument doc = JsonWire.Document(bytes); JsonElement root = doc.RootElement;
        JsonElement libraries = root.GetProperty("libraries");
        if (!libraries.TryGetProperty("Microsoft.Data.SqlClient/7.0.2", out _) || !libraries.TryGetProperty("Apache.Arrow/23.0.0", out _))
            throw new InvalidDataException("startup.dependency_version");
        JsonElement target = root.GetProperty("targets").GetProperty(root.GetProperty("runtimeTarget").GetProperty("name").GetString()!);
        var resolved = new HashSet<string>(StringComparer.Ordinal);
        foreach (JsonProperty library in target.EnumerateObject())
            foreach (string category in new[] { "runtime", "native", "runtimeTargets", "resources" })
                if (library.Value.TryGetProperty(category, out JsonElement assets))
                    foreach (JsonProperty asset in assets.EnumerateObject())
                    {
                        DeploymentManifest.Relative(asset.Name);
                        string[] parts = asset.Name.Split('/'); int lib = Array.IndexOf(parts, "lib");
                        string reduced = lib >= 0 ? string.Join('/', parts.Skip(lib + 2)) : asset.Name;
                        string? matched = new[] { asset.Name, reduced, Path.GetFileName(asset.Name) }.FirstOrDefault(inventory.Contains);
                        if (matched is null) throw new InvalidDataException("startup.dependency_missing");
                        resolved.Add(matched);
                    }
        if (inventory.Any(name => (name.EndsWith(".dll", StringComparison.Ordinal) || Path.GetFileName(name).Contains(".so", StringComparison.Ordinal)) && !resolved.Contains(name)))
            throw new InvalidDataException("startup.unlisted_resolution_asset");
    }
}
