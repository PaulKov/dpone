using System.Text;
using System.Text.Json;
using Dpone.SqlClient.Input;

internal sealed record LiveCase(string Name, NativeInputContract Contract, string File, string[][]? Expected, bool Malformed);

internal static class LiveCases
{
    internal static IEnumerable<LiveCase> Read(string root)
    {
        using var cases = JsonDocument.Parse(File.ReadAllText(Path.Combine(root, "live-cases.json")));
        foreach (var item in cases.RootElement.EnumerateArray())
        {
            var columns = item.GetProperty("contract").GetProperty("columns").EnumerateArray().Select(c => NativeColumn.Admit(
                c.GetProperty("name").GetString()!, c.GetProperty("source_type").GetString()!, c.GetProperty("nullable").GetBoolean(),
                c.GetProperty("storage_type").GetString()!, c.GetProperty("prefix_width").GetInt32(), Number(c,"fixed_length"),
                Number(c,"precision"), Number(c,"scale"), c.GetProperty("encoding").GetString())).ToArray();
            var contract = new NativeInputContract(columns, new ExpectedInput(item.GetProperty("rows").GetInt64(), item.GetProperty("bytes").GetInt64(),
                item.GetProperty("sha256").GetString()!), new NativeInputLimits(item.GetProperty("bound").GetInt32()));
            yield return new LiveCase(item.GetProperty("name").GetString()!, contract, Path.Combine(root,"fixtures",item.GetProperty("file").GetString()!),
                item.GetProperty("expected").Deserialize<string[][]>(), item.GetProperty("malformed").GetBoolean());
        }
    }
    internal static string Canonical(object? value, NativeColumn column) => value switch {
        DBNull or null => "null", long number => number.ToString(), double number => BitConverter.DoubleToInt64Bits(number).ToString(),
        DateTime date => date.Ticks.ToString(), string text => Convert.ToBase64String(Encoding.Unicode.GetBytes(text)),
        _ => throw new InvalidDataException("live.scalar") };
    internal static string Quote(string identifier) => "[" + identifier.Replace("]", "]]") + "]";
    private static int? Number(JsonElement c, string n) => c.GetProperty(n).ValueKind == JsonValueKind.Null ? null : c.GetProperty(n).GetInt32();
}
