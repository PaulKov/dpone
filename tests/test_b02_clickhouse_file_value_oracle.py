"""Local protocol peers beneath both real adapters; independent value reconciliation.

The peer decodes RowBinary without importing a production encoder or decoder.
It is a protocol fixture, not live ClickHouse certification.
"""

import base64
import hashlib
import inspect
import io
import json
import logging
import os
import re
import shlex
import struct
import sys
from contextlib import contextmanager
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from time import monotonic
from urllib.parse import parse_qs, urlsplit

import pytest

from dpone.config.load_config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.connectors.clickhouse_bulk import ClickHouseClientCredentials, ClickHouseClientOptions
from dpone.runtime.connectors.clickhouse_file_stage_client import ClickHouseFileClientRunner
from dpone.runtime.connectors.clickhouse_file_stage_http import ClickHouseFileHttpRunner
from dpone.runtime.connectors.clickhouse_http_bulk import ClickHouseHttpCredentials, ClickHouseHttpOptions
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.clickhouse_validated_file_models import ClickHouseValidatedFilePolicy, FileConsumptionError
from dpone.runtime.sinks.load_payload import LoadPayload

SCHEMA = (("id", "int"), ("text", "nvarchar(max) nullable"), ("binary", "varbinary(max) nullable"), ("after", "int"))
TARGET = (("id", "Int32"), ("text", "Nullable(String)"), ("binary", "Nullable(String)"), ("after", "Int32"))
SERVER = "11111111-1111-4111-8111-111111111111"
DATABASE = "22222222-2222-4222-8222-222222222222"
TABLE = "33333333-3333-4333-8333-333333333333"


def read_exact(stream, count):
    data = stream.read(count)
    if len(data) != count:
        raise ValueError("truncated independent RowBinary")
    return data


def decode_rows(data, *, prefixes=None):
    stream, rows = io.BytesIO(data), []
    while stream.tell() < len(data):
        row = [struct.unpack("<i", read_exact(stream, 4))[0]]
        for index in range(2):
            tag = read_exact(stream, 1)
            if tag == b"\x01":
                row.append(None)
                continue
            if tag != b"\x00":
                raise ValueError("noncanonical nullable flag")
            size, shift = 0, 0
            prefix = bytearray()
            while True:
                byte = read_exact(stream, 1)[0]
                prefix.append(byte)
                size |= (byte & 127) << shift
                if byte < 128:
                    break
                shift += 7
                if shift > 63:
                    raise ValueError("invalid length")
            if prefixes is not None:
                prefixes.append((row[0], index, size, bytes(prefix)))
            value = read_exact(stream, size)
            row.append(value.decode("utf-8") if index == 0 else value)
        if struct.unpack("<i", read_exact(stream, 4))[0] != -row[0]:
            raise ValueError("independent trailing sentinel mismatch")
        rows.append(tuple(row))
    return rows


def expected_wire(rows):
    result = bytearray()
    for identifier, text, binary in rows:
        result.extend(struct.pack("<i", identifier))
        for value in (text.encode() if text is not None else None, binary):
            result.append(int(value is None))
            if value is None:
                continue
            size = len(value)
            while size >= 128:
                result.append((size & 127) | 128)
                size >>= 7
            result.append(size)
            result.extend(value)
        result.extend(struct.pack("<i", -identifier))
    return bytes(result)


class NoGenericIO:
    def get_records(self, *_args, **_kwargs):
        raise AssertionError("explicit stage used generic get_records")

    def execute_query(self, *_args, **_kwargs):
        raise AssertionError("explicit stage used generic target mutation")


class ProtocolPeer:
    """Persist actual queries and bytes; derive COUNT from independently decoded rows."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.path = self.directory / "peer.json"
        self.after_query = None

    def state(self):
        return json.loads(self.path.read_text()) if self.path.exists() else {"queries": [], "rows": 0}

    def query(self, sql, data, settings):
        state = self.state()
        state["queries"].append(sql)
        assert settings["output_format_json_quote_64bit_integers"] == "0"
        assert settings["async_insert"] == "0"
        assert settings["input_format_allow_errors_num"] == "0"
        assert settings["input_format_allow_errors_ratio"] == "0"
        fault, rows = state.get("fault"), []
        if "system.databases" in sql:
            rows = [[SERVER, "sample", DATABASE, "Atomic"]]
        elif sql.startswith("CREATE"):
            state["table"] = re.search(r"`(__dpone_b02_[a-f0-9]+)`", sql)[1]
            state["marker"] = re.search(r"COMMENT '([^']+)'", sql)[1]
        elif "system.tables" in sql:
            if "table" in state:
                rows = [[SERVER, "sample", state["table"], TABLE, state["marker"], "MergeTree", 0]]
        elif "system.columns" in sql:
            rows = [[name, dtype, index, "", ""] for index, (name, dtype) in enumerate(state.get("schema", TARGET), 1)]
        elif sql.startswith("INSERT"):
            (self.directory / "received.bin").write_bytes(data)
            if "scalar_width" in state:
                width = state["scalar_width"]
                if len(data) % width:
                    raise ValueError("truncated independent scalar")
                state["rows"] = len(data) // width
            else:
                state["rows"] = len(decode_rows(data))
        elif sql.startswith("SELECT count()"):
            rows = [[state["rows"]]]
            if fault == "count_bool":
                rows = [[True]]
            if fault == "count_string":
                rows = [[str(state["rows"])]]
            if fault == "count_missing":
                rows = []
            if fault == "count_extra":
                rows = [[state["rows"]], [state["rows"]]]
            if fault in {"count_wrong", "drop_unknown"}:
                rows = [[state["rows"] + 1]]
        elif sql.startswith("DROP"):
            state.pop("table", None)
        elif sql.startswith("KILL"):
            if fault == "insert_cancelled":
                rows = [["finished", re.search(r"query_id = '([^']+)'", sql)[1]]]
        else:
            raise AssertionError(sql)
        self.path.write_text(json.dumps(state))
        if self.after_query is not None:
            self.after_query(sql)
        if sql.startswith("CREATE") and fault == "create_unknown":
            return b"Code: 999. synthetic CREATE error"
        if sql.startswith("DROP") and fault == "drop_unknown":
            return b"Code: 999. synthetic DROP error"
        if sql.startswith("INSERT") and fault in {"insert_unknown", "insert_cancelled"}:
            return b"Code: 999. synthetic peer error"
        return b"".join(json.dumps(row).encode() + b"\n" for row in rows)


@contextmanager
def peer_runner(tmp_path, mode, *, clock=monotonic):
    peer = ProtocolPeer(tmp_path)
    if mode == "client":
        script = tmp_path / "protocol_peer.py"
        fixture_source = "\n".join(
            [
                "import io, json, re, struct, sys\nfrom pathlib import Path",
                f"SERVER={SERVER!r}\nDATABASE={DATABASE!r}\nTABLE={TABLE!r}\nTARGET={TARGET!r}",
                inspect.getsource(read_exact),
                inspect.getsource(decode_rows),
                inspect.getsource(ProtocolPeer),
                "peer = ProtocolPeer(sys.argv[1])\narguments = sys.argv[2:]",
                "settings = dict(zip((value.removeprefix('--') for value in arguments[::2]), arguments[1::2], strict=True))",
                "sys.stdout.buffer.write(peer.query(settings['query'], sys.stdin.buffer.read(), settings))",
            ]
        )
        script.write_text(fixture_source, encoding="utf-8")
        command = f"{shlex.quote(sys.executable)} {shlex.quote(str(script))} {shlex.quote(str(tmp_path))}"
        runner = ClickHouseFileClientRunner(
            ClickHouseClientCredentials("127.0.0.1", 9000, "sample", "test"),
            ClickHouseClientOptions(client_command=command, input_format="RowBinary"),
            clock=clock,
        )
        yield peer, runner
        return

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            parameters = {key: values[0] for key, values in parse_qs(urlsplit(self.path).query).items()}
            body = bytearray()
            while True:
                size = int(self.rfile.readline(), 16)
                if not size:
                    self.rfile.readline()
                    break
                body.extend(read_exact(self.rfile, size))
                assert self.rfile.read(2) == b"\r\n"
            result = peer.query(parameters["query"], bytes(body), parameters)
            self.send_response(200)
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            self.wfile.write(result)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        runner = ClickHouseFileHttpRunner(
            ClickHouseHttpCredentials("127.0.0.1", server.server_port, "sample", "test"),
            ClickHouseHttpOptions(input_format="RowBinary"),
            clock=clock,
        )
        yield peer, runner
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def payload_config(tmp_path, mode, rows, encoding="none"):
    def cell(value):
        if value is None:
            return b""
        text = value.hex() if isinstance(value, bytes) else str(value)
        # Independent spelling of the approved producer's wire, including literals.
        if text == "":
            return b"\x1dE"
        escapes = {"\x1d": "\x1dP", "\t": "\x1dT", "\n": "\x1dN", "\r": "\x1dR", "\x1f": "\x1dU", "\x1e": "\x1dS"}
        return "".join(escapes.get(char, char) for char in text).encode()

    wire = b"".join(b"\t".join(cell(value) for value in (*row, -row[0])) + b"\n" for row in rows)
    path = tmp_path / "source.bcp"
    path.write_bytes(wire)
    raw = FileExportArtifact(
        str(path),
        [name for name, _ in SCHEMA],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=len(rows),
        estimated_rows=999,
    )
    contract = SchemaContract.from_config({"enforcement": "strict", "columns": {}})
    receipt = validate_mssql_delimited_file_contract(raw, schema=SCHEMA, contract=contract)
    wrapper = ContractValidatedFileArtifact(raw, contract=contract, schema=SCHEMA, run_id="test", load_id="test")
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="sample",
        source_table="rows",
        target_schema="sample",
        target_table="rows",
        options={
            "clickhouse_bulk": {"mode": mode},
            "type_fidelity": {"binary_encoding": encoding},
            "physical_design": {"columns": {name: {"target_type": {"clickhouse": dtype}} for name, dtype in TARGET}},
        },
    )
    policy = ClickHouseValidatedFilePolicy(work_directory=tmp_path / "attempts", max_spool_bytes=8_388_608)
    return LoadPayload(wrapper, SCHEMA), config, policy, raw, receipt, wire


def run_public(runner, config, payload, policy):
    sink = ClickHouseSink(
        NoGenericIO(),
        logger=logging.getLogger("b02-oracle"),
        validated_file_runner_factory=lambda _config, _policy: runner,
    )
    return sink.stage_validated_file(config, payload, policy=policy)


def case_rows():
    texts = [
        None,
        "",
        "tab\ttext",
        "line\ntext",
        "carriage\rtext",
        "\x1d",
        "\x1f|\x1e",
        "|".join("\x1d" + letter for letter in "ETNRUSP"),
        '"quoted"|a""b|\'single\'',
        r"\N|\t|\n|\r|\\|\x1dE",
        "é|e\u0301|Ж|😀",
        "\x1d\x1dT\t\x1dE",
        r"\N",
        "NULL",
        " ",
        "\x1dE",
    ]
    rows = [(1000 + index, text, b"\x00\xff") for index, text in enumerate(texts, 1)]
    rows += [
        (2000 + index, "binary", value)
        for index, value in enumerate((None, b"", b"\x00", b"\xff", b"\x00\xff", bytes(range(256))))
    ]
    rows += [(3000 + index, "x" * size, b"z" * size) for index, size in enumerate((127, 128, 16383, 16384))]
    rows += [rows[0], rows[0]]
    return rows


@pytest.mark.parametrize("mode", ["client", "http"])
def test_public_stage_independent_value_reconciliation(tmp_path, mode):
    cases = []
    for encoding in ("none", "hex", "base64"):
        folder = tmp_path / encoding
        folder.mkdir()
        rows = case_rows()
        transformed = [
            (
                identifier,
                text,
                value
                if value is None or encoding == "none"
                else (value.hex().encode() if encoding == "hex" else base64.b64encode(value)),
            )
            for identifier, text, value in rows
        ]
        payload, config, policy, raw, receipt, wire = payload_config(folder, mode, rows, encoding)
        with peer_runner(folder, mode) as (peer, runner):
            handle = run_public(runner, config, payload, policy)
        received = (folder / "received.bin").read_bytes()
        prefixes = []
        observed = decode_rows(received, prefixes=prefixes)
        for index, (size, prefix) in enumerate(
            ((127, b"\x7f"), (128, b"\x80\x01"), (16383, b"\xff\x7f"), (16384, b"\x80\x80\x01"))
        ):
            assert (3000 + index, 0, size, prefix) in prefixes
        assert observed == transformed
        assert received == expected_wire(transformed)
        assert handle.staged_rows == len(rows) == payload.artifact.validation_summary.accepted_rows
        assert raw.contract_validation_receipt is receipt
        assert Path(raw.file_path).read_bytes() == wire
        queries = peer.state()["queries"]
        assert sum(query.startswith("INSERT") for query in queries) == 1
        assert sum(query.startswith("SELECT count()") for query in queries) == 1
        evidence = handle.metadata["validated_file_consumption"]
        assert evidence["outcome"] == "staged"
        assert not list(Path(evidence["journal_directory"]).glob("transport*"))
        for ordinal, (expected, actual) in enumerate(zip(transformed, observed, strict=True), 1):
            cases.append(
                {
                    "case": f"{encoding}/row-{ordinal}/id-{expected[0]}",
                    "row_ordinal": ordinal,
                    "expected": [expected[0], expected[1], None if expected[2] is None else expected[2].hex()],
                    "observed": [actual[0], actual[1], None if actual[2] is None else actual[2].hex()],
                }
            )
        cases.append(
            {
                "case": f"{encoding}/wire",
                "expected_sha256": hashlib.sha256(expected_wire(transformed)).hexdigest(),
                "observed_sha256": hashlib.sha256(received).hexdigest(),
            }
        )
    output = Path(os.environ.get("DPONE_TEST_B02_EVIDENCE_ROOT", str(tmp_path / "evidence"))) / mode
    output.mkdir(parents=True, exist_ok=True)
    candidate = os.environ.get("DPONE_TEST_B02_CANDIDATE", "UNVERIFIED/WIP")
    (output / "value-reconciliation.json").write_text(
        json.dumps(
            {
                "candidate_sha": candidate,
                "mode": mode,
                "non_live": True,
                "status": "PASS",
                "test_identity": f"{Path(__file__).name}::test_public_stage_independent_value_reconciliation[{mode}]",
                "scope": "text_binary_value_reconciliation",
                "cases": cases,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


@pytest.mark.parametrize("mode", ["client", "http"])
def test_zero_rows_skips_insert_but_counts_empty_table(tmp_path, mode):
    payload, config, policy, _raw, _receipt, _wire = payload_config(tmp_path, mode, [])
    with peer_runner(tmp_path, mode) as (peer, runner):
        handle = run_public(runner, config, payload, policy)
    assert handle.staged_rows == 0
    evidence = handle.metadata["validated_file_consumption"]
    assert evidence["derived_identity"]["transport_size_bytes"] == 0
    assert evidence["derived_identity"]["transport_sha256"] == hashlib.sha256(b"").hexdigest()
    assert evidence["query_kind"] == "insert" and evidence["query_id"] is None
    assert evidence["local_sender_state"] == evidence["remote_execution_state"] == "not_submitted_empty"
    assert not any(query.startswith("INSERT") for query in peer.state()["queries"])
    assert sum(query.startswith("SELECT count()") for query in peer.state()["queries"]) == 1


@pytest.mark.parametrize("mode", ["client", "http"])
@pytest.mark.parametrize(
    "fault",
    [
        "count_bool",
        "count_string",
        "count_missing",
        "count_extra",
        "count_wrong",
        "insert_unknown",
        "insert_cancelled",
        "drop_unknown",
        "create_unknown",
    ],
)
def test_failed_transport_or_count_never_grants_stage(tmp_path, mode, fault):
    payload, config, policy, raw, receipt, wire = payload_config(tmp_path, mode, [(1, "text", b"bytes")])
    with peer_runner(tmp_path, mode) as (peer, runner):
        peer.path.write_text(json.dumps({"queries": [], "rows": 0, "fault": fault}))
        with pytest.raises((RuntimeError, FileConsumptionError)) as error:
            run_public(runner, config, payload, policy)
    record = error.value.details["validated_file_consumption"]
    unknown = fault in {"insert_unknown", "drop_unknown", "create_unknown"}
    assert record["outcome"] == ("retained_unknown" if unknown else "failed_cleaned")
    assert bool(list(Path(record["journal_directory"]).glob("transport*"))) == unknown
    assert sum(query.startswith("DROP") for query in peer.state()["queries"]) == (
        0 if fault in {"insert_unknown", "create_unknown"} else 1
    )
    assert sum(query.startswith("INSERT") for query in peer.state()["queries"]) == (
        0 if fault == "create_unknown" else 1
    )
    assert payload.artifact.validation_summary.accepted_rows == 0
    assert raw.contract_validation_receipt is receipt and Path(raw.file_path).read_bytes() == wire


@pytest.mark.parametrize(
    "change", ["nullable", "truncated", "trailing", "reordered", "corrupt", "dropped", "duplicated"]
)
def test_independent_oracle_rejects_changed_bytes(change):
    rows = [(1, "text", b"bytes"), (2, "", None)]
    wire = expected_wire(rows)
    changed = {
        "nullable": wire[:4] + b"\x02" + wire[5:],
        "truncated": wire[:-1],
        "trailing": wire + b"x",
        "reordered": expected_wire(rows[::-1]),
        "dropped": expected_wire(rows[:1]),
        "duplicated": expected_wire(rows + rows[-1:]),
        "corrupt": wire[:6] + b"z" + wire[7:],
    }[change]
    try:
        observed = decode_rows(changed)
    except ValueError:
        return
    assert observed != rows


@pytest.mark.parametrize(
    "phase", ["preparing", "prepared", "preddl", "boundary_event", "count", "verification_combined"]
)
@pytest.mark.parametrize("empty", [False, True])
def test_one_budget_covers_preparation_and_full_verification(tmp_path, phase, empty):
    from dataclasses import replace

    from dpone.runtime.immutable_local_tree import materialize_immutable_local_tree_at
    from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
    from dpone.runtime.sinks.clickhouse_validated_file_ingestion import ClickHouseValidatedFileService
    from dpone.runtime.sinks.clickhouse_validated_file_journal import ClickHouseFileAttemptJournal
    from dpone.runtime.storage_policy import StoragePreflightService

    now = [monotonic()]

    def clock():
        return now[0]

    payload, config, policy, _raw, _receipt, _wire = payload_config(
        tmp_path, "http", [] if empty else [(1, "text", b"bytes")]
    )
    policy = replace(policy, preparation_timeout_seconds=10, verification_timeout_seconds=10)
    advanced = set()

    def after_query(sql):
        key = None
        if phase == "preddl" and "system.databases" in sql:
            key = "preddl"
        elif phase in {"count", "verification_combined"} and sql.startswith("SELECT count()"):
            key = "count"
        elif phase == "verification_combined" and "count" in advanced and "system.tables" in sql:
            key = "owner"
        if key is not None and key not in advanced:
            advanced.add(key)
            now[0] += 6 if phase == "verification_combined" else 11

    def write_event(descriptor, relative, files, **kwargs):
        result = materialize_immutable_local_tree_at(descriptor, relative, files, **kwargs)
        event = json.loads(files["attempt.json"])
        expired_event = event["phase"] == phase or (
            phase == "boundary_event"
            and event["query_kind"] == "insert"
            and event["remote_execution_state"] in {"completed", "not_submitted_empty"}
        )
        if expired_event and phase not in advanced:
            advanced.add(phase)
            now[0] += 11
        return result

    def journal_factory(policy, attempt_id, *, storage):
        return ClickHouseFileAttemptJournal(policy, attempt_id, storage=storage, event_writer=write_event)

    with peer_runner(tmp_path, "http", clock=clock) as (peer, runner):
        peer.after_query = after_query
        service = ClickHouseValidatedFileService(
            runner_factory=lambda _config, _policy: runner,
            resolver=ClickHousePhysicalColumnTypeResolver(),
            clock=clock,
            journal_factory=partial(journal_factory, storage=StoragePreflightService()),
        )
        with pytest.raises(TimeoutError):
            service.stage(config, payload, policy=policy)
    assert payload.artifact.validation_summary.accepted_rows == 0
    mutations = [sql for sql in peer.state()["queries"] if sql.startswith(("CREATE", "INSERT", "DROP"))]
    if phase in {"preparing", "prepared", "preddl"}:
        assert mutations == []
    else:
        assert sum(sql.startswith("CREATE") for sql in mutations) == 1
        assert sum(sql.startswith("INSERT") for sql in mutations) == int(not empty)
        assert sum(sql.startswith("DROP") for sql in mutations) == 1
    assert not list(policy.work_directory.glob("*/transport*"))


def test_genuine_codec_matches_independent_source_bytes(tmp_path):
    rows = case_rows()
    _payload, _config, _policy, _raw, _receipt, independent = payload_config(tmp_path, "http", rows)
    codec = BulkTextCodec()

    def cell(value):
        if value is None:
            return b""
        value = value.hex() if isinstance(value, bytes) else str(value)
        return codec.encode(value).encode()

    produced = b"".join(b"\t".join(cell(value) for value in (*row, -row[0])) + b"\n" for row in rows)
    assert produced == independent


@pytest.mark.parametrize("mutation", ["create", "insert", "drop", "final"])
@pytest.mark.parametrize("write_point", ["before", "after"])
def test_journal_failure_prevents_affected_mutation_or_success(tmp_path, mutation, write_point):
    from dpone.runtime.immutable_local_tree import materialize_immutable_local_tree_at
    from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
    from dpone.runtime.sinks.clickhouse_validated_file_ingestion import ClickHouseValidatedFileService
    from dpone.runtime.sinks.clickhouse_validated_file_journal import ClickHouseFileAttemptJournal
    from dpone.runtime.storage_policy import StoragePreflightService

    payload, config, policy, _raw, _receipt, _wire = payload_config(
        tmp_path, "http", [(1, "secret-like-value", b"bytes")]
    )

    def write_event(descriptor, relative, files, **kwargs):
        event = json.loads(files["attempt.json"])
        failing = (event["query_kind"] == mutation and event["remote_execution_state"] == "not_submitted") or (
            mutation == "final" and event["outcome"] == "staged"
        )
        if failing and write_point == "before":
            raise OSError("synthetic durable write failure")
        result = materialize_immutable_local_tree_at(descriptor, relative, files, **kwargs)
        if failing:
            raise OSError("synthetic fsync acknowledgment failure")
        return result

    def journal_factory(policy, attempt_id, *, storage):
        return ClickHouseFileAttemptJournal(policy, attempt_id, storage=storage, event_writer=write_event)

    with peer_runner(tmp_path, "http") as (peer, runner):
        if mutation == "drop":
            peer.path.write_text(json.dumps({"queries": [], "rows": 0, "fault": "count_wrong"}))
        service = ClickHouseValidatedFileService(
            runner_factory=lambda _config, _policy: runner,
            resolver=ClickHousePhysicalColumnTypeResolver(),
            clock=monotonic,
            journal_factory=partial(journal_factory, storage=StoragePreflightService()),
        )
        with pytest.raises(FileConsumptionError) as error:
            service.stage(config, payload, policy=policy)
    assert payload.artifact.validation_summary.accepted_rows == 0
    queries = peer.state()["queries"]
    if mutation != "final":
        assert not any(sql.startswith(mutation.upper()) for sql in queries)
    record = error.value.details["validated_file_consumption"]
    assert record["outcome"] != "staged"
    assert "secret-like-value" not in json.dumps(record)
    assert error.value.blocker == ("staging_count_mismatch" if mutation == "drop" else "attempt_journal_unavailable")


@pytest.mark.parametrize("mode", ["client", "http"])
def test_public_stage_exact_scalar_matrix(tmp_path, mode):
    cases = [
        ("tinyint", "UInt8", "0", b"\x00"),
        ("tinyint", "UInt8", "255", b"\xff"),
        ("smallint", "Int16", "-32768", b"\x00\x80"),
        ("smallint", "Int16", "32767", b"\xff\x7f"),
        ("int", "Int32", "-2147483648", b"\x00\x00\x00\x80"),
        ("int", "Int32", "2147483647", b"\xff\xff\xff\x7f"),
        ("bigint", "Int64", "-9223372036854775808", b"\x00" * 7 + b"\x80"),
        ("bigint", "Int64", "9223372036854775807", b"\xff" * 7 + b"\x7f"),
        ("bit", "Bool", "0", b"\x00"),
        ("bit", "Bool", "1", b"\x01"),
        ("decimal(9,2)", "Decimal(9,2)", "-9999999.99", (-999999999).to_bytes(4, "little", signed=True)),
        ("decimal(18,2)", "Decimal(18,2)", "123.45", (12345).to_bytes(8, "little", signed=True)),
        ("decimal(38,4)", "Decimal(38,4)", "-123.4567", (-1234567).to_bytes(16, "little", signed=True)),
        ("decimal(76,10)", "Decimal(76,10)", "0.0000000001", (1).to_bytes(32, "little", signed=True)),
        ("int nullable", "Nullable(Int32)", "", b"\x01"),
        ("decimal(9,2) nullable", "Nullable(Decimal(9,2))", "", b"\x01"),
    ]
    for index, (dtype, target, value, expected) in enumerate(cases):
        folder = tmp_path / str(index)
        folder.mkdir()
        source = folder / "source.bcp"
        wire = value.encode() + b"\n"
        source.write_bytes(wire)
        schema = (("value", dtype),)
        raw = FileExportArtifact(
            str(source), ["value"], format="mssql-delimited", bulk_text_codec=BulkTextCodec(), rows_exported=1
        )
        contract = SchemaContract.from_config({"enforcement": "strict", "columns": {}})
        validate_mssql_delimited_file_contract(raw, schema=schema, contract=contract)
        wrapper = ContractValidatedFileArtifact(
            raw, contract=contract, schema=schema, run_id="scalar", load_id="scalar"
        )
        config = LoadConfig(
            source_conn_id="source",
            target_conn_id="target",
            source_schema="sample",
            source_table="rows",
            target_schema="sample",
            target_table="rows",
            options={
                "clickhouse_bulk": {"mode": mode},
                "physical_design": {"columns": {"value": {"target_type": {"clickhouse": target}}}},
            },
        )
        policy = ClickHouseValidatedFilePolicy(work_directory=folder / "attempts", max_spool_bytes=1_048_576)
        with peer_runner(folder, mode) as (peer, runner):
            peer.path.write_text(
                json.dumps({"queries": [], "rows": 0, "schema": [["value", target]], "scalar_width": len(expected)})
            )
            handle = run_public(runner, config, LoadPayload(wrapper, schema), policy)
        assert (folder / "received.bin").read_bytes() == expected
        assert handle.staged_rows == wrapper.validation_summary.accepted_rows == 1
        assert source.read_bytes() == wire


@pytest.mark.parametrize("boundary", ["preddl", "insert"])
@pytest.mark.parametrize("change", ["bytes", "receipt", "columns", "codec", "config", "transport"])
def test_changed_identity_never_returns_stage(tmp_path, boundary, change):
    payload, config, policy, raw, _receipt, wire = payload_config(tmp_path, "http", [(1, "text", b"bytes")])
    changed = False

    def after_query(sql):
        nonlocal changed
        trigger = "system.databases" in sql if boundary == "preddl" else sql.startswith("INSERT")
        if changed or not trigger:
            return
        changed = True
        if change == "bytes":
            Path(raw.file_path).write_bytes(wire.replace(b"text", b"next"))
        elif change == "receipt":
            contract = SchemaContract.from_config({"enforcement": "strict", "columns": {}})
            validate_mssql_delimited_file_contract(raw, schema=SCHEMA, contract=contract)
        elif change == "columns":
            raw.columns = ("changed", "text", "binary", "after")
        elif change == "codec":
            raw.bulk_text_codec = BulkTextCodec(marker_prefix="~", empty_string_marker="~E")
        elif change == "config":
            config.options["physical_design"]["columns"]["id"]["target_type"]["clickhouse"] = "Int64"
        else:
            spool = next(policy.work_directory.glob("*/transport.rowbinary"))
            with spool.open("r+b") as handle:
                handle.write(b"changed")

    with peer_runner(tmp_path, "http") as (peer, runner):
        peer.after_query = after_query
        with pytest.raises(RuntimeError) as error:
            run_public(runner, config, payload, policy)
    assert changed and payload.artifact.validation_summary.accepted_rows == 0
    queries = peer.state()["queries"]
    assert sum(sql.startswith("CREATE") for sql in queries) == int(boundary == "insert")
    assert sum(sql.startswith("DROP") for sql in queries) == int(boundary == "insert")
    assert error.value.details["validated_file_consumption"]["outcome"] == "failed_cleaned"
