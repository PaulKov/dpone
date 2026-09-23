using Dpone.SqlClient.Input;
using System.Security.Cryptography;

internal static class InputChecks
{
    static int Main(string[] args)
    {
        if (args.Length != 1) throw new ArgumentException("Expected generated corpus directory");
        var column = NativeColumn.Admit("arbitrary [name]", "bigint", false, "bigint", 0, 8, null, null, null);
        byte[] bytes = BitConverter.GetBytes(long.MinValue);
        var contract = new NativeInputContract(new[] { column }, new ExpectedInput(1, 8, Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant()), new NativeInputLimits(64));
        using var stream = new MemoryStream(bytes);
        using var reader = new NativeInputReader(stream, contract);
        if (!reader.HasRows || !reader.Read() || reader.GetInt64(0) != long.MinValue || reader.Read()) throw new Exception("valid_row");
        if (reader.RequireComplete().Rows != 1 || !stream.CanRead) throw new Exception("receipt_or_ownership");
        Console.WriteLine("PASS initial exact scalar/EOF/stream ownership");
        CorpusChecks.Run(args[0]);
        BoundaryChecks.Run();
        BudgetChecks.Run();
        return 0;
    }
}
