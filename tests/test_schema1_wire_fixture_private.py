"""Independent fixture assembler limits and exact scalar/frame framing."""

import hashlib
import json

import pytest

from tests.schema1_wire_fixture_private import assemble_fixture, load_fixture


def _frame(*items):
    return {"tag": "frame", "domain": "dpone-test", "items": list(items)}


def _write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    return path


def test_scalar_framing_is_independent_and_exact():
    document = _frame({"tag": "null"}, {"tag": "bool", "value": True}, {"tag": "int", "value": 7})
    assert assemble_fixture(document) == b"dpone-test\0\0\0\0\1n\0\0\0\1t\0\0\0\11i\0\0\0\0\0\0\0\7"


def test_named_hash_uses_readable_frame_preimage():
    preimage = _frame({"tag": "text", "value": "fixture"})
    document = _frame({"tag": "sha256_frame", "role": "test preimage", "frame": preimage})
    digest = hashlib.sha256(assemble_fixture(preimage)).digest()
    assert assemble_fixture(document).endswith(b"b" + (32).to_bytes(8, "big") + digest)


@pytest.mark.parametrize(
    "node",
    [
        {"tag": "bytes", "hex": "00"},
        {"tag": "null", "extra": None},
        {"tag": "int", "value": True},
        {"tag": "int", "value": 2**63},
        {"tag": "bool", "value": 1},
        {"tag": "text", "value": "e\u0301"},
        {"tag": "text", "value": "x" * 8193},
        {"tag": "uuid", "value": "invalid"},
        {"tag": "tuple", "items": {}},
        {"tag": "synthetic_digest", "role": "D1", "byte": 34},
        {"tag": "synthetic_digest", "role": {}, "byte": 17},
        {"tag": "sha256_frame", "role": "test", "frame": {"tag": "null"}},
    ],
)
def test_malformed_or_opaque_node_rejected(node):
    with pytest.raises(ValueError):
        assemble_fixture(_frame(node))


def test_depth_and_node_limits_reject():
    deep = {"tag": "null"}
    for _ in range(25):
        deep = {"tag": "tuple", "items": [deep]}
    for document in (_frame(deep), _frame(*[{"tag": "null"}] * 4097)):
        with pytest.raises(ValueError, match="limit"):
            assemble_fixture(document)


def test_document_order_duplicates_and_limits_reject(tmp_path):
    path = tmp_path / "fixture.json"
    for raw in ('{"tag":"frame","tag":"frame"}', '{"items":[],"domain":"dpone-test","tag":"frame"}', "x" * 262145):
        path.write_text(raw)
        with pytest.raises(ValueError):
            load_fixture(path)
    path.write_bytes(b"\xff")
    with pytest.raises(ValueError):
        load_fixture(path)


def test_regular_canonical_json_and_symlink_rules(tmp_path):
    path = _write(tmp_path / "fixture.json", _frame())
    assert load_fixture(path) == b"dpone-test\0"
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(ValueError):
        load_fixture(link)


def test_unknown_root_and_domain_reject():
    for document in ({"tag": "null"}, {"tag": "frame", "domain": "dpone-test\0", "items": []}):
        with pytest.raises(ValueError):
            assemble_fixture(document)


@pytest.mark.parametrize(
    "kind,pinned",
    [
        ("contract", "ae2b4facf7462d31cd9abf24e3c051516f68e978785ae405ee83d5d9e98dd3b2"),
        ("attestation", "cff26e4ff4f0872e55a3a69a710b803ae62cce2cdde196dfc5666e93a1d75e06"),
    ],
)
def test_readable_fixtures_preserve_historical_wire_hashes(kind, pinned):
    from pathlib import Path

    path = Path(__file__).parent / "fixtures" / f"postgres_mssql_r1_v3_schema_1_{kind}.json"
    assert hashlib.sha256(load_fixture(path)).hexdigest() == pinned


def test_output_size_limit_rejects():
    with pytest.raises(ValueError, match="limit"):
        assemble_fixture(_frame(*[{"tag": "text", "value": "x" * 8192}] * 33))
