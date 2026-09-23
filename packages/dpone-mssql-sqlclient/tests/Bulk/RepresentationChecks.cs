using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Dpone.SqlClient.Bulk;
using Dpone.SqlClient.Input;

internal static class RepresentationChecks
{
    internal static void Run(string corpusRoot)
    {
        using var cases = JsonDocument.Parse(File.ReadAllText(Path.Combine(corpusRoot, "cases.json")));
        using var expected = JsonDocument.Parse(File.ReadAllText(Path.Combine(corpusRoot, "python-reference.json")));
        int checks = 0;
        foreach (string mode in new[] { "rows", "arrow" })
        foreach (var test in cases.RootElement.EnumerateArray())
        {
            string name = test.GetProperty("name").GetString()!;
            var reference = expected.RootElement.GetProperty(name);
            string outcome = "REJECTED";
            var values = new List<string?[]>();
            InputBufferBudget? budget = null;
            DbDataReader? reader = null;
            InputReceipt? receipt = null;
            try
            {
                NativeColumn[] columns = test.GetProperty("contract").GetProperty("columns").EnumerateArray().Select(c => NativeColumn.Admit(
                    c.GetProperty("name").GetString()!, c.GetProperty("source_type").GetString()!, c.GetProperty("nullable").GetBoolean(),
                    c.GetProperty("storage_type").GetString()!, c.GetProperty("prefix_width").GetInt32(), Number(c,"fixed_length"),
                    Number(c,"precision"), Number(c,"scale"), c.GetProperty("encoding").GetString())).ToArray();
                var contract = new NativeInputContract(columns, new ExpectedInput(test.GetProperty("rows").GetInt64(),
                    test.GetProperty("bytes").GetInt64(), test.GetProperty("sha256").GetString()!), new NativeInputLimits(test.GetProperty("bound").GetInt32()));
                var policy = new BulkInputPolicy(3, 67108864);
                using var file = File.OpenRead(Path.Combine(corpusRoot, "fixtures", test.GetProperty("file").GetString()!));
                budget = new InputBufferBudget(policy.MaxInputBatchBytes);
                reader = mode == "rows" ? new NativeInputReader(file, contract, default, budget) : new BoundedArrowReader(file, contract, policy, budget);
                int stop = test.GetProperty("stopAfter").GetInt32();
                _ = reader.HasRows;
                while ((stop < 0 || values.Count < stop) && reader.Read())
                {
                    var row = new string?[reader.FieldCount];
                    for (int i = 0; i < row.Length; i++) row[i] = reader.IsDBNull(i) ? null : columns[i].DataTypeName switch {
                        "bigint" => reader.GetInt64(i).ToString(), "float(53)" => BitConverter.DoubleToInt64Bits(reader.GetDouble(i)).ToString(),
                        "nvarchar(max)" => Convert.ToBase64String(Encoding.Unicode.GetBytes(reader.GetString(i))),
                        _ => reader.GetDateTime(i).Ticks.ToString() };
                    values.Add(row);
                    if (budget.UsedBytes > budget.LimitBytes) throw new Exception("budget_exceeded");
                }
                receipt = reader is NativeInputReader native ? native.RequireComplete() : ((BoundedArrowReader)reader).RequireComplete();
                if (reader is BoundedArrowReader arrow && arrow.MaxObservedBatchRows > 3) throw new Exception("batch_rows");
                outcome = "COMPLETE";
            }
            catch (InvalidDataException) { }
            finally { reader?.Dispose(); }
            if (budget is not null && budget.UsedBytes != 0) throw new Exception("lease_leak:" + name + ":" + mode);
            if (outcome != reference.GetProperty("status").GetString()) throw new Exception("status:" + name + ":" + mode);
            if (outcome == "COMPLETE" && (JsonSerializer.Serialize(values) != JsonSerializer.Serialize(reference.GetProperty("values")) ||
                JsonSerializer.Serialize(receipt) != JsonSerializer.Serialize(reference.GetProperty("receipt")))) throw new Exception("fidelity:" + name + ":" + mode);
            checks++;
        }
        Console.WriteLine($"PASS {checks} rows/Arrow differential cases with row caps and zero retained budget after disposal");
    }
    private static int? Number(JsonElement c, string name) => c.GetProperty(name).ValueKind == JsonValueKind.Null ? null : c.GetProperty(name).GetInt32();
}
