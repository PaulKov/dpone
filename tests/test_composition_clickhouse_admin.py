"""Actual loopback administrative HTTP tests; never live ClickHouse proof."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from urllib.parse import parse_qs, urlsplit

import pytest

from dpone.adapters.composition_clickhouse_admin import _QUERIES, ClickHousePrincipalHttpClient
from dpone.adapters.composition_clickhouse_transport import ClickHouseTransportCredentials
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_clickhouse_transport import server as server

USER = "dpone_ch_" + "a" * 64
CREATE = f"CREATE USER `{USER}` IDENTIFIED WITH sha256_hash BY '{'b' * 64}' HOST NONE DEFAULT ROLE NONE GRANTEES NONE"


def client(endpoint, **kwargs):
    return ClickHousePrincipalHttpClient(
        endpoint=endpoint,
        credentials=ClickHouseTransportCredentials("admin", "secret"),
        timeout_seconds=kwargs.pop("timeout_seconds", 2),
        **kwargs,
    )


def test_command_posts_fixed_sql_once_without_hash_in_url(server):
    endpoint, requests, _ = server
    client(endpoint).command(CREATE, parameters={}, redactions=("b" * 64,))
    assert len(requests) == 1
    path, _, body = requests[0]
    assert body.decode() == CREATE and "b" * 64 not in path
    assert parse_qs(urlsplit(path).query)["wait_end_of_query"] == ["1"]


@pytest.mark.parametrize("statement", ["SELECT 1", "DROP USER default", CREATE + "; SELECT 1"])
def test_unknown_command_sends_nothing(server, statement):
    endpoint, requests, _ = server
    with pytest.raises(CompositionAdmissionError):
        client(endpoint).command(statement, parameters={}, redactions=())
    assert not requests


IDENTITY = "SELECT toString(id) FROM system.users WHERE name={user:String} LIMIT 2"


def document(columns=("toString(id)",), types=("String",), data=None):
    rows = [["11111111-1111-1111-1111-111111111111"]] if data is None else data
    return {
        "meta": [{"name": name, "type": kind} for name, kind in zip(columns, types, strict=True)],
        "data": rows,
        "rows": len(rows),
        "statistics": {"elapsed": 0.001, "rows_read": len(rows), "bytes_read": 64},
    }


def test_query_decodes_complete_rows_and_sets_bounded_outer_limit(server):
    endpoint, requests, behavior = server
    behavior["body"] = canonical_json_bytes(document())
    result = client(endpoint).query(IDENTITY, parameters={"user": USER}, max_rows=2)
    assert result == (("11111111-1111-1111-1111-111111111111",),)
    path, _, body = requests[0]
    assert body.decode() == f"SELECT * FROM ({IDENTITY}) LIMIT 3 FORMAT JSONCompact"
    assert parse_qs(urlsplit(path).query)["param_user"] == [USER]
    assert "query" not in parse_qs(urlsplit(path).query)


@pytest.mark.parametrize("statement", list(_QUERIES))
def test_every_principal_query_uses_exact_metadata_and_types(server, statement):
    endpoint, _, behavior = server
    key, columns, types = _QUERIES[statement]
    values = {"String": "value", "Nullable(String)": None, "Array(String)": ["host"], "UInt8": 0, "UInt64": "0"}
    behavior["body"] = canonical_json_bytes(document(columns, types, [[values[kind] for kind in types]]))
    parameters = {} if key is None else {key: USER if key == "user" else "target_db"}
    observed = client(endpoint).query(statement, parameters=parameters, max_rows=2)
    assert len(observed) == 1 and len(observed[0]) == len(columns)
    if types == ("UInt64",):
        assert observed == ((0,),)


@pytest.mark.parametrize(
    "change",
    [
        "name",
        "type",
        "extra_meta",
        "missing_meta",
        "row_count",
        "extra_rows",
        "row_width",
        "row_object",
        "cell_type",
        "cell_budget",
        "extra_key",
        "exception",
        "stats",
        "negative_stats",
    ],
)
def test_invalid_json_compact_is_never_an_observation(server, change):
    endpoint, requests, behavior = server
    value = document()
    if change in {"name", "type"}:
        value["meta"][0][change] = "unexpected"
    elif change == "extra_meta":
        value["meta"].append({"name": "x", "type": "String"})
    elif change == "missing_meta":
        value.pop("meta")
    elif change == "row_count":
        value["rows"] = 2
    elif change == "extra_rows":
        value["data"] *= 3
        value["rows"] = 3
    elif change == "row_width":
        value["data"][0].append("x")
    elif change == "row_object":
        value["data"] = [{"toString(id)": "x"}]
    elif change in {"cell_type", "cell_budget"}:
        value["data"][0][0] = 1 if change == "cell_type" else "x" * 4097
    elif change in {"exception", "extra_key"}:
        value[change] = "secret error"
    elif change == "stats":
        value["statistics"]["other"] = 0
    else:
        value["statistics"]["elapsed"] = -1
    behavior["body"] = canonical_json_bytes(value)
    with pytest.raises(CompositionAdmissionError, match="query_unknown") as error:
        client(endpoint).query(IDENTITY, parameters={"user": USER}, max_rows=2)
    assert "secret" not in str(error.value) and len(requests) == 1


@pytest.mark.parametrize(
    "body", [b"{", b"{} trailing", b'{"meta":[],"meta":[]}', b'{"rows":NaN}', b"\xff", b"Code: 1. secret"]
)
def test_non_json_or_ambiguous_json_is_sanitized(server, body):
    endpoint, requests, behavior = server
    behavior["body"] = body
    with pytest.raises(CompositionAdmissionError, match="query_unknown") as error:
        client(endpoint).query(IDENTITY, parameters={"user": USER}, max_rows=2)
    assert "secret" not in str(error.value) and len(requests) == 1


@pytest.mark.parametrize("method", ["query", "command"])
@pytest.mark.parametrize(
    "change",
    [
        "redirect",
        "error_header",
        "partial",
        "disconnect",
        "query_id",
        "gzip",
        "duplicate_length",
        "duplicate_exception",
        "no_framing",
        "bad_chunk",
        "oversize",
    ],
)
def test_uncertain_admin_response_never_retries(server, method, change):
    endpoint, requests, behavior = server
    behavior.update(
        {
            "redirect": {"status": 302},
            "error_header": {"error": "241"},
            "partial": {"declared": 100, "body": b"part"},
            "disconnect": {"disconnect": True},
            "query_id": {"query_id": "wrong"},
            "gzip": {"extra_headers": [("Content-Encoding", "gzip")]},
            "duplicate_length": {"extra_headers": [("Content-Length", "0")]},
            "duplicate_exception": {
                "extra_headers": [("X-ClickHouse-Exception-Code", "0"), ("X-ClickHouse-Exception-Code", "0")]
            },
            "no_framing": {"declared": "absent"},
            "bad_chunk": {"chunked": b"0\r\n"},
            "oversize": {"body": b"x" * 65},
        }[change]
    )
    admin = client(endpoint, max_response_bytes=64)
    with pytest.raises(CompositionAdmissionError, match="unknown") as error:
        if method == "command":
            admin.command(CREATE, parameters={}, redactions=("b" * 64,))
        else:
            admin.query(IDENTITY, parameters={"user": USER}, max_rows=2)
    assert len(requests) == 1 and "secret" not in str(error.value) and "b" * 64 not in str(error.value)


@pytest.mark.parametrize("stage", ["header", "response"])
def test_admin_absolute_deadline_bounds_waiting_for_headers_and_body(server, stage):
    endpoint, requests, behavior = server
    entered, release = Event(), Event()
    behavior.update({stage + "_entered": entered, stage + "_release": release, "chunked": b"0\r\n\r\n"})
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(
            client(endpoint, timeout_seconds=0.1).command, CREATE, parameters={}, redactions=("b" * 64,)
        )
        assert entered.wait(2)
        try:
            with pytest.raises(CompositionAdmissionError, match="unknown"):
                pending.result(timeout=2)
        finally:
            release.set()
    assert len(requests) == 1


@pytest.mark.parametrize(
    "parameters,max_rows",
    [
        ({"user": USER, "session_id": "x"}, 2),
        ({"user": "default"}, 2),
        ({"user": USER}, 0),
        ({"user": USER}, 65),
        ({"user": USER}, True),
        ({}, 2),
    ],
)
def test_invalid_query_subject_or_limit_never_sends(server, parameters, max_rows):
    endpoint, requests, _ = server
    with pytest.raises(CompositionAdmissionError):
        client(endpoint).query(IDENTITY, parameters=parameters, max_rows=max_rows)
    assert not requests


@pytest.mark.parametrize(
    "statement",
    [
        f"ALTER USER `{USER}` HOST ANY",
        f"GRANT ALL ON *.* TO `{USER}`",
        CREATE.replace("HOST NONE", "HOST ANY"),
        CREATE.replace("sha256_hash", "plaintext_password"),
        f"ALTER USER `{USER}` HOST IP '0.0.0.0'",
        f"ALTER USER `{USER}` HOST IP 'invalid'",
        f"GRANT INSERT ON `db`.`x;DROP TABLE t` TO `{USER}`",
    ],
)
def test_admin_command_surface_cannot_expand(server, statement):
    endpoint, requests, _ = server
    with pytest.raises(CompositionAdmissionError):
        client(endpoint).command(statement, parameters={}, redactions=("b" * 64,))
    assert not requests


def test_query_pins_overflow_to_throw_and_rejects_reported_hidden_rows(server):
    endpoint, requests, behavior = server
    value = document()
    value["rows_before_limit_at_least"] = 3
    behavior["body"] = canonical_json_bytes(value)
    with pytest.raises(CompositionAdmissionError):
        client(endpoint).query(IDENTITY, parameters={"user": USER}, max_rows=2)
    settings = parse_qs(urlsplit(requests[0][0]).query)
    assert settings["max_result_rows"] == ["3"]
    assert all(
        settings[key] == ["throw"] for key in ("result_overflow_mode", "read_overflow_mode", "timeout_overflow_mode")
    )


@pytest.mark.parametrize("value", [-1, True, "00", "-1", "18446744073709551616", 2**64])
def test_unsigned_count_overflow_or_ambiguous_value_rejects(server, value):
    endpoint, _, behavior = server
    behavior["body"] = canonical_json_bytes(document(("count()",), ("UInt64",), [[value]]))
    with pytest.raises(CompositionAdmissionError):
        client(endpoint).query("SELECT count() FROM system.transactions", parameters={}, max_rows=1)


def test_complete_chunked_query_waits_for_final_framing(server):
    endpoint, _, behavior = server
    body = canonical_json_bytes(document())
    behavior["chunked"] = f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n"
    assert client(endpoint).query(IDENTITY, parameters={"user": USER}, max_rows=2) == (
        ("11111111-1111-1111-1111-111111111111",),
    )
