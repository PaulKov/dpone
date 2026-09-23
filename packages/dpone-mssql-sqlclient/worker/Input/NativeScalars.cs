using System.Buffers.Binary;
using System.Text;

namespace Dpone.SqlClient.Input;

internal static class NativeScalars
{
    private static readonly UnicodeEncoding StrictUnicode = new(false, false, true);

    internal static object Decode(NativeColumn column, byte[] payload)
    {
        switch (column.DataTypeName)
        {
            case "bigint": return BinaryPrimitives.ReadInt64LittleEndian(payload);
            case "float(53)":
                double value = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(payload));
                if (!double.IsFinite(value)) throw new InvalidDataException("input.nonfinite_float");
                return value;
            case "nvarchar(max)":
                try { return StrictUnicode.GetString(payload); }
                catch (DecoderFallbackException) { throw new InvalidDataException("input.invalid_utf16"); }
            case "datetime2(6)":
                long ticks = 0;
                for (int i = 0; i < 5; i++) ticks |= (long)payload[i] << (8 * i);
                int days = payload[5] | payload[6] << 8 | payload[7] << 16;
                if (ticks >= TimeSpan.TicksPerDay || ticks % 10 != 0 || days > 3652058)
                    throw new InvalidDataException("input.invalid_temporal");
                return new DateTime(checked(days * TimeSpan.TicksPerDay + ticks), DateTimeKind.Unspecified);
            default: throw new InvalidDataException("input.unsupported_layout");
        }
    }
}
