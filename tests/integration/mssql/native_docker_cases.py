"""SQL literals and independently expected ClickHouse values for native BCP."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NativeCase:
    name: str
    source: str
    target: str
    values: tuple[tuple[str, str], ...]
    projection: str = "toString(value)"
    collation: str = ""
    reject: str = ""


def native_cases() -> list[NativeCase]:
    cases = []
    for source, target, low, high in [
        ("bit", "UInt8", 0, 1),
        ("tinyint", "UInt8", 0, 255),
        ("smallint", "Int16", -32768, 32767),
        ("int", "Int32", -(2**31), 2**31 - 1),
        ("bigint", "Int64", -(2**63), 2**63 - 1),
    ]:
        cases.append(NativeCase(source, source, target, tuple((str(x), str(x)) for x in (low, 0, high))))
    for source, target in [("real", "Float32"), ("float(24)", "Float64"), ("float(53)", "Float64")]:
        cases.append(NativeCase(source, source, target, (("-1.5", "-1.5"), ("0", "0"), ("1.25", "1.25"))))
    for p in (1, 9, 10, 19, 20, 28, 29, 38):
        for scale in (0, p):
            maximum = "9" * p if not scale else "0." + "9" * p
            zero = "0" if not scale else "0." + "0" * p
            source = f"decimal({p},{scale})"
            cases.append(
                NativeCase(
                    source, source, f"Decimal({p},{scale})", tuple((x, x) for x in ("-" + maximum, zero, maximum))
                )
            )
    for source, precision, low, high in [
        ("money", 19, "-922337203685477.5808", "922337203685477.5807"),
        ("smallmoney", 10, "-214748.3648", "214748.3647"),
    ]:
        cases.append(
            NativeCase(source, source, f"Decimal({precision},4)", tuple((x, x) for x in (low, "0.0000", high)))
        )
    uuids = (
        "00000000-0000-0000-0000-000000000000",
        "00112233-4455-6677-8899-aabbccddeeff",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
    )
    cases.append(NativeCase("uuid", "uniqueidentifier", "UUID", tuple(("'" + x + "'", x) for x in uuids)))
    cases.append(
        NativeCase(
            "date", "date", "Date32", tuple(("'" + x + "'", x) for x in ("1900-01-01", "1970-01-01", "2299-12-31"))
        )
    )
    cases.append(
        NativeCase(
            "datetime",
            "datetime",
            "DateTime64(3, 'UTC')",
            (
                ("'1969-12-31T23:59:59.997'", "1969-12-31 23:59:59.997"),
                ("'2000-01-01T00:00:00.003'", "2000-01-01 00:00:00.003"),
            ),
        )
    )
    cases.append(
        NativeCase(
            "smalldatetime",
            "smalldatetime",
            "DateTime64(0, 'UTC')",
            (("'1900-01-01'", "1900-01-01 00:00:00"), ("'2079-06-06T23:59:00'", "2079-06-06 23:59:00")),
        )
    )
    for scale in range(8):
        fraction = "." + "9" * scale if scale else ""
        zero = "." + "0" * scale if scale else ""
        cases.append(
            NativeCase(
                f"time{scale}",
                f"time({scale})",
                "String",
                (("'00:00:00'", "00:00:00" + zero), ("'23:59:59" + fraction + "'", "23:59:59" + fraction)),
            )
        )
        cases.append(
            NativeCase(
                f"datetime2_{scale}",
                f"datetime2({scale})",
                f"DateTime64({scale}, 'UTC')",
                (
                    ("'1969-12-31T23:59:59" + fraction + "'", "1969-12-31 23:59:59" + fraction),
                    ("'2024-02-29T00:00:00'", "2024-02-29 00:00:00" + zero),
                ),
            )
        )
        cases.append(
            NativeCase(
                f"offset{scale}",
                f"datetimeoffset({scale})",
                f"DateTime64({scale}, 'UTC')",
                (
                    ("'1970-01-01T00:00:00+14:00'", "1969-12-31 10:00:00" + zero),
                    ("'1970-01-01T23:59:59" + fraction + "-14:00'", "1970-01-02 13:59:59" + fraction),
                ),
            )
        )
    for source in ("varchar(20)", "varchar(max)", "nvarchar(20)", "nvarchar(max)"):
        cases.append(
            NativeCase(
                source,
                source,
                "String",
                (("''", ""), ("N'A'+NCHAR(0)+NCHAR(9)+NCHAR(10)+N'  '", "4100090A2020")),
                "hex(value)",
            )
        )
    for source, text, collation in [
        ("varchar(1)", "é", "Latin1_General_100_CI_AS"),
        ("char(4)", "café", "Latin1_General_100_CI_AS"),
        ("varchar(40)", "café", "Latin1_General_100_CI_AS"),
        ("varchar(40)", "Привет", "Cyrillic_General_100_CI_AS"),
        ("varchar(40)", "é😀", "Latin1_General_100_CI_AS_SC_UTF8"),
        ("nvarchar(40)", "é😀", ""),
    ]:
        cases.append(
            NativeCase(
                source + collation,
                source,
                "String",
                (("N'" + text + "'", text.encode().hex().upper()),),
                "hex(value)",
                collation,
            )
        )
    for source in ("char(4)", "nchar(4)"):
        cases.append(
            NativeCase(source, source, "FixedString(4)", (("'A'", "41202020"), ("''", "20202020")), "hex(value)")
        )
    for source in ("binary(4)", "varbinary(4)", "varbinary(max)"):
        fixed = source.startswith("binary")
        cases.append(
            NativeCase(
                source,
                source,
                "String",
                (("0x", "00000000" if fixed else ""), ("0x00FF41", "00FF4100" if fixed else "00FF41")),
                "hex(value)",
            )
        )
    for source, size in [
        ("varchar(8000)", 8000),
        ("nvarchar(4000)", 4000),
        *(("varchar(max)", x) for x in (65535, 65536, 65537)),
        ("varbinary(max)", 65537),
    ]:
        expression = f"CONVERT({source}, REPLICATE(CAST('A' AS varchar(max)),{size}))"
        cases.append(
            NativeCase(source + str(size), source, "String", ((expression, str(size)),), "toString(length(value))")
        )
    for source, target, value in [
        ("date", "Date32", "0001-01-01"),
        ("date", "Date32", "9999-12-31"),
        ("datetime", "DateTime64(3)", "1753-01-01"),
        ("datetime2(7)", "DateTime64(7)", "1899-12-31T23:59:59.9999999"),
        ("datetime2(7)", "DateTime64(7)", "2300-01-01"),
    ]:
        cases.append(
            NativeCase(
                "reject_" + source + value,
                source,
                target,
                (("'" + value + "'", value),),
                reject="temporal_out_of_range",
            )
        )
    cases.append(
        NativeCase("integer_narrowing", "bigint", "Int32", (("2147483648", "2147483648"),), reject="out_of_range")
    )
    cases.append(
        NativeCase(
            "all_binary_bytes",
            "varbinary(max)",
            "String",
            (("0x" + bytes(range(256)).hex(), bytes(range(256)).hex().upper()),),
            "hex(value)",
        )
    )
    cases.append(
        NativeCase(
            "float32_extremes",
            "real",
            "Float32",
            (
                ("3.4028234663852886e38", "FFFF7F7F"),
                ("-3.4028234663852886e38", "FFFF7FFF"),
                ("1.1754943508222875e-38", "00008000"),
            ),
            "hex(reinterpretAsFixedString(value))",
        )
    )
    cases.append(
        NativeCase(
            "float64_extremes",
            "float(53)",
            "Float64",
            (("1.7976931348623157e308", "FFFFFFFFFFFFEF7F"), ("-1.7976931348623157e308", "FFFFFFFFFFFFEFFF")),
            "hex(reinterpretAsFixedString(value))",
        )
    )
    return cases
