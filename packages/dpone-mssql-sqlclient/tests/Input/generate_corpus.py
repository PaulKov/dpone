"""Independent synthetic byte producer and actual main Python decoder oracle."""

import argparse
import base64
import dataclasses
import datetime
import hashlib
import io
import json
import struct
import sys
from pathlib import Path

from dpone.runtime.mssql_tds_decoder import MssqlTdsRowDecoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def generate(output: Path) -> None:
    """Generate synthetic cases and bind the actual Python reference modules."""
    P = output
    P.mkdir(parents=True, exist_ok=True)
    (P / "fixtures").mkdir(exist_ok=True)
    CASES = []
    EXPECTED = {}
    REFERENCES = {}
    TEXT = [None, "", "\0", "a\0b", "\0\0", "😀Жe\u0301  ", "x" * 9000 + "😀", "\0a", "a\0", "last"]
    FLOAT = [
        0.0,
        -0.0,
        float.fromhex("0x1.fffffffffffffp1023"),
        -float.fromhex("0x1.fffffffffffffp1023"),
        float.fromhex("0x0.0000000000001p-1022"),
        -float.fromhex("0x0.0000000000001p-1022"),
        float.fromhex("0x1p-1022"),
        1.2345678901234567,
        -1.5,
        42.0,
    ]
    INT = [-(2**63), 2**63 - 1, 0, -1, 1, 255, 256, -256, 65535, -65535]
    DATES = [
        datetime.datetime.min,
        datetime.datetime.max,
        datetime.datetime(2000, 2, 29, 12, 34, 56, 123456),
        datetime.datetime(1970, 1, 1),
        datetime.datetime(2026, 9, 15),
    ]

    def canonical(value):
        if value is None:
            return None
        if type(value) is float:
            return str(struct.unpack("<q", struct.pack("<d", value))[0])
        if type(value) is str:
            return base64.b64encode(value.encode("utf-16le")).decode()
        if isinstance(value, datetime.datetime):
            delta = value - datetime.datetime.min
            return str((delta.days * 86400 + delta.seconds) * 10000000 + delta.microseconds * 10)
        return str(value)

    def encode(value, column):
        if value is None:
            return (-1).to_bytes(column.prefix_width, "little", signed=True)
        kind = column.storage_type
        if kind == "bigint":
            payload = struct.pack("<q", value)
        elif kind == "float":
            payload = struct.pack("<d", value)
        elif kind == "nvarchar":
            payload = value.encode("utf-16le")
        else:
            days = (value.date() - datetime.date.min).days
            ticks = ((value.hour * 60 + value.minute) * 60 + value.second) * 10000000 + value.microsecond * 10
            payload = ticks.to_bytes(5, "little") + days.to_bytes(3, "little")
        return (
            len(payload).to_bytes(column.prefix_width, "little", signed=True) if column.prefix_width else b""
        ) + payload

    def add(name, contract, data, rows, original=None, **overrides):
        path = P / "fixtures" / f"{name}.native"
        path.write_bytes(data)
        case = {
            "name": name,
            "contract": contract.to_dict(),
            "file": path.name,
            "bound": 2_000_000,
            "rows": rows,
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "stopAfter": -1,
        }
        case.update(overrides)
        CASES.append(case)
        received = []
        observed = bytearray()
        status = "COMPLETE"
        error = None
        receipt = None
        try:
            decoder = MssqlTdsRowDecoder(contract, input_mode="rows")
            iterator = decoder.iter_rows(io.BytesIO(data), max_row_bytes=case["bound"], on_bytes=observed.extend)
            if case["stopAfter"] >= 0:
                for _ in range(case["stopAfter"]):
                    received.append(next(iterator))
                raise ValueError("input_incomplete")
            received = list(iterator)
            if (len(received), len(observed), hashlib.sha256(observed).hexdigest()) != (
                case["rows"],
                case["bytes"],
                case["sha256"],
            ):
                raise ValueError("input_identity_mismatch")
            receipt = {"Rows": len(received), "Bytes": len(observed), "Sha256": hashlib.sha256(observed).hexdigest()}
        except Exception as exc:
            status = "REJECTED"
            error = type(exc).__name__ + ":" + str(exc)
        REFERENCES[name] = {
            "status": status,
            "values": [[canonical(v) for v in row] for row in received],
            "receipt": receipt,
            "error": error,
            "observedBytes": len(observed),
        }
        if original is not None:
            EXPECTED[name] = [[canonical(v) for v in row] for row in original]
            assert status == "COMPLETE" and REFERENCES[name]["values"] == EXPECTED[name], name
        return case

    def contract(schema):
        return build_mssql_bcp_native_contract(schema=schema, query="synthetic native bridge only")

    # Deliberately non-periodic kind order and names; nullability differs from type.
    import random

    for width in (5, 100):
        kinds = (
            ["nvarchar(max)", "datetime2(6)", "bigint", "float(53)", "bigint"]
            if width == 5
            else (["float(53)", "bigint", "datetime2(6)", "nvarchar(max)"] * 25)
        )
        if width == 100:
            random.Random(451).shuffle(kinds)
        schema = [(f"field [{i}] Ж space", k + (" nullable" if i % 3 != 1 else "")) for i, k in enumerate(kinds)]
        c = contract(schema)
        for count in (0, 1, 10):
            rows = []
            for r in range(count):
                row = []
                for col in c.columns:
                    v = {
                        "bigint": INT[r],
                        "float": FLOAT[r],
                        "nvarchar": TEXT[r] if TEXT[r] is not None else "",
                        "datetime2": DATES[r % 5],
                    }[col.storage_type]
                    if col.nullable and r == 9:
                        v = None
                    row.append(v)
                rows.append(row)
            data = b"".join(encode(v, col) for row in rows for v, col in zip(row, c.columns))
            add(f"valid{width}_{count}", c, data, count, rows)
            if count == 10:
                add(f"early{width}", c, data, count, stopAfter=1)
                add(f"stop_at_count{width}", c, data, count, stopAfter=count)
                add(f"wrong_hash{width}", c, data, count, sha256="0" * 64)
                add(f"wrong_bytes{width}", c, data, count, bytes=len(data) + 1)
                add(f"wrong_rows{width}", c, data, count + 1)
    # Explicit NULL for every nullable admitted type and empty string distinction.
    c = contract([(k + " name", k + " nullable") for k in ["bigint", "float(53)", "nvarchar(max)", "datetime2(6)"]])
    add("all_null", c, b"".join(encode(None, col) for col in c.columns), 1, [[None] * 4])
    # Single-field negatives make preallocation failure instrumentation unambiguous.
    for typ in ["bigint", "float(53)", "datetime2(6)"]:
        c = contract([("x", typ + " nullable")])
        for prefix in [-2, 0, 7, 9, 127]:
            add(typ.split("(")[0] + "_prefix_" + str(prefix), c, struct.pack("b", prefix) + b"\0" * 16, 1)
        add(typ.split("(")[0] + "_truncated", c, b"\x08" + b"\0" * 7, 1)
    c = contract([("text", "nvarchar(max) nullable")])
    for n in [-2, -(2**63), 2**63 - 1, 2_000_001]:
        add("text_length_" + str(n), c, struct.pack("<q", n), 1)
    for name, payload in [
        ("odd", b"a"),
        ("high_surrogate", b"\x00\xd8"),
        ("low_surrogate", b"\x00\xdc"),
        ("wrong_pair", b"\x00\xd8a\0"),
    ]:
        add("utf16_" + name, c, struct.pack("<q", len(payload)) + payload, 1)
    for n in range(1, 8):
        add("truncated_prefix_" + str(n), c, b"\0" * n, 1)
    add("truncated_text", c, struct.pack("<q", 100) + b"x" * 4, 1)
    add("text_row_overrun", c, struct.pack("<q", 10) + b"\0" * 10, 1, bound=17)
    add("text_exact_bound", c, struct.pack("<q", 10) + b"\0" * 10, 1, [["\0" * 5]], bound=18)
    add("prefix_bound", c, b"\0" * 8, 1, bound=7)
    c = contract([("text", "nvarchar(max)")])
    add("nonnull_null", c, b"\xff" * 8, 1)
    c = contract([("time", "datetime2(6)")])
    for name, ticks, days in [("submicro", 1, 0), ("day_overflow", 864000000000, 0), ("date_overflow", 0, 3652059)]:
        add(name, c, ticks.to_bytes(5, "little") + days.to_bytes(3, "little"), 1)
    c = contract([("double", "float(53)")])
    for name, value in [("nan", float("nan")), ("positive_inf", float("inf")), ("negative_inf", -float("inf"))]:
        add(name, c, struct.pack("<d", value), 1)
    c = contract([("a", "bigint"), ("b", "float(53)"), ("c", "datetime2(6)"), ("d", "nvarchar(max)")])
    row = [INT[0], FLOAT[1], DATES[2], "😀\0"]
    data = b"".join(encode(v, col) for v, col in zip(row, c.columns))
    add("mixed_exact_bound", c, data, 1, [row], bound=len(data))
    add("mixed_row_overrun", c, data, 1, bound=len(data) - 1)
    for n in range(1, len(data)):
        add("truncate_mixed_" + str(n), c, data[:n], 1)
    # Full physical metadata corruption is rejected by both implementations pre-read.
    for key, value in [
        ("prefix_width", 1),
        ("fixed_length", 7),
        ("scale", 6),
        ("storage_type", "float"),
        ("nullable", True),
    ]:
        c = contract([("time", "datetime2(6)")])
        bad = dataclasses.replace(c, columns=(dataclasses.replace(c.columns[0], **{key: value}),))
        add("metadata_" + key, bad, b"\0" * 8, 1)
    (P / "cases.json").write_text(json.dumps(CASES, ensure_ascii=False, indent=2))
    (P / "python-reference.json").write_text(json.dumps(REFERENCES, indent=2))
    (P / "original-oracle.json").write_text(json.dumps(EXPECTED, indent=2))
    mods = {
        name: str(module.__file__)
        for name, module in sys.modules.items()
        if name.startswith("dpone.") and getattr(module, "__file__", None)
    }
    (P / "reference-manifest.json").write_text(
        json.dumps(
            {
                "python": sys.version,
                "modules": {
                    name: {"path": path, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}
                    for name, path in mods.items()
                },
            },
            indent=2,
        )
    )
    print(
        json.dumps(
            {
                "cases": len(CASES),
                "referenceComplete": sum(v["status"] == "COMPLETE" for v in REFERENCES.values()),
                "originalOracles": len(EXPECTED),
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    generate(parser.parse_args().output)
