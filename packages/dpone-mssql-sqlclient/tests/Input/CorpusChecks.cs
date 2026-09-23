using System.Text;
using System.Text.Json;
using Dpone.SqlClient.Input;

internal static class CorpusChecks
{
    internal static void Run(string directory)
    {
        using var cases = JsonDocument.Parse(File.ReadAllText(Path.Combine(directory, "cases.json")));
        using var reference = JsonDocument.Parse(File.ReadAllText(Path.Combine(directory, "python-reference.json")));
        using var oracle = JsonDocument.Parse(File.ReadAllText(Path.Combine(directory, "original-oracle.json")));
        var records = new List<object>();
        int count = 0;
        foreach (JsonElement test in cases.RootElement.EnumerateArray())
        {
            string name = test.GetProperty("name").GetString()!;
            var values = new List<string?[]>();
            string status = "REJECTED";
            InputReceipt? receipt = null;
            NativeInputReader? reader = null;
            string? error = null;
            try
            {
                var columns = test.GetProperty("contract").GetProperty("columns").EnumerateArray().Select(c => NativeColumn.Admit(
                    c.GetProperty("name").GetString()!, c.GetProperty("source_type").GetString()!, c.GetProperty("nullable").GetBoolean(),
                    c.GetProperty("storage_type").GetString()!, c.GetProperty("prefix_width").GetInt32(), Number(c,"fixed_length"),
                    Number(c,"precision"), Number(c,"scale"), c.GetProperty("encoding").GetString())).ToArray();
                var contract = new NativeInputContract(columns, new ExpectedInput(test.GetProperty("rows").GetInt64(),
                    test.GetProperty("bytes").GetInt64(), test.GetProperty("sha256").GetString()!), new NativeInputLimits(test.GetProperty("bound").GetInt32()));
                using var file = File.OpenRead(Path.Combine(directory, "fixtures", test.GetProperty("file").GetString()!));
                reader = new NativeInputReader(file, contract);
                int stop = test.GetProperty("stopAfter").GetInt32();
                // Exercise the actual consumer lookahead without consuming its first row.
                if (stop != 0) _ = reader.HasRows;
                while ((stop < 0 || values.Count < stop) && reader.Read())
                {
                    var row = new string?[reader.FieldCount];
                    for (int i = 0; i < row.Length; i++)
                        row[i] = reader.IsDBNull(i) ? null : columns[i].DataTypeName switch {
                            "bigint" => reader.GetInt64(i).ToString(),
                            "float(53)" => BitConverter.DoubleToInt64Bits(reader.GetDouble(i)).ToString(),
                            "nvarchar(max)" => Convert.ToBase64String(Encoding.Unicode.GetBytes(reader.GetString(i))),
                            "datetime2(6)" => reader.GetDateTime(i).Ticks.ToString(), _ => throw new Exception("type") };
                    values.Add(row);
                }
                receipt = reader.RequireComplete();
                status = "COMPLETE";
            }
            catch (Exception e) { error = e.Message; }
            finally { reader?.Dispose(); }
            JsonElement expected = reference.RootElement.GetProperty(name);
            if (status != expected.GetProperty("status").GetString()) throw new Exception("corpus.status:" + name);
            // Rejection may occur earlier than Python while respecting allocation limits.
            if (status == "COMPLETE")
            {
                if (JsonSerializer.Serialize(values) != JsonSerializer.Serialize(expected.GetProperty("values")))
                    throw new Exception("corpus.python_values:" + name);
                if (oracle.RootElement.TryGetProperty(name, out var original) && JsonSerializer.Serialize(values) != JsonSerializer.Serialize(original))
                    throw new Exception("corpus.original_values:" + name);
                if (JsonSerializer.Serialize(receipt) != JsonSerializer.Serialize(expected.GetProperty("receipt")))
                    throw new Exception("corpus.receipt:" + name);
            }
            else if (reader is not null)
            {
                try { reader.RequireComplete(); throw new Exception("corpus.false_receipt:" + name); }
                catch (InvalidDataException) { }
            }
            records.Add(new { name, status, receipt, error });
            count++;
        }
        File.WriteAllText(Path.Combine(directory, "corpus-results.json"), JsonSerializer.Serialize(records, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"PASS corpus {count} cases against Python/reference and original oracle");
    }
    private static int? Number(JsonElement c, string name) => c.GetProperty(name).ValueKind == JsonValueKind.Null ? null : c.GetProperty(name).GetInt32();
}
