from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


codec = _load(
    "dpone_release_candidate_evidence_codec_test",
    "tools/agent_policy/release_candidate_evidence_codec.py",
)


def test_canonical_json_is_stable_utf8_finite_and_lf_terminated() -> None:
    payload = {"z": [3, {"b": False, "a": "да"}], "a": 1}

    raw = codec.canonical_json_bytes(payload)

    assert raw == b'{"a":1,"z":[3,{"a":"\xd0\xb4\xd0\xb0","b":false}]}\n'
    assert codec.sha256_bytes(raw).startswith("sha256:")
    assert len(codec.sha256_bytes(raw)) == 71
    assert codec.canonical_json_sha256(payload) == codec.sha256_bytes(raw)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_canonical_json_rejects_non_finite_numbers_at_any_depth(value: float) -> None:
    with pytest.raises(ValueError, match="non-finite"):
        codec.canonical_json_bytes({"nested": [value]})


@pytest.mark.parametrize(
    "raw",
    [
        b'{"status":"PASS","status":"FAIL"}',
        b'{"outer":{"status":"PASS","status":"FAIL"}}',
    ],
)
def test_strict_json_rejects_duplicate_keys_at_every_depth(raw: bytes) -> None:
    with pytest.raises(ValueError, match="duplicate JSON key 'status'"):
        codec.strict_json_object(raw, field="authority")


@pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
def test_strict_json_rejects_non_standard_non_finite_numbers(constant: bytes) -> None:
    with pytest.raises(ValueError, match="non-finite JSON number"):
        codec.strict_json_object(b'{"metric":' + constant + b"}", field="authority")


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b'"PASS"',
        b"null",
        b"{} trailing",
        b"\xff",
    ],
)
def test_strict_json_requires_exactly_one_utf8_object(raw: bytes) -> None:
    with pytest.raises(ValueError, match="JSON object|UTF-8 JSON object"):
        codec.strict_json_object(raw, field="authority")


def test_strict_json_rejects_oversized_input_before_decode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(codec, "MAX_JSON_BYTES", 8)

    with pytest.raises(ValueError, match="8-byte limit"):
        codec.strict_json_object(b'{"long":1}', field="authority")


def test_write_canonical_json_is_create_once_and_read_is_byte_exact(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "authority.json"
    payload = {"status": "PASS", "run_attempt": 2}

    written = codec.write_canonical_json(output, payload)
    decoded, observed = codec.read_strict_json(output, field="authority")

    assert observed == written == codec.canonical_json_bytes(payload)
    assert decoded == payload
    with pytest.raises(ValueError, match="already exists"):
        codec.write_canonical_json(output, payload)
    assert output.read_bytes() == written


def test_closed_field_set_reports_missing_and_unknown_fields() -> None:
    with pytest.raises(ValueError, match=r"missing=\['release'\].*unknown=\['passed'\]"):
        codec.require_exact_keys(
            {"status": "PASS", "passed": True},
            frozenset({"status", "release"}),
            field="receipt",
        )


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.0, "1", None])
def test_positive_integer_rejects_bool_and_non_positive_or_non_integer_values(value: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        codec.require_positive_int(value, field="run_id")


@pytest.mark.parametrize("value", [True, False, 0, -1, math.nan, math.inf, "1", None])
def test_positive_number_rejects_bool_non_finite_and_non_positive_values(value: object) -> None:
    with pytest.raises(ValueError, match="positive finite number"):
        codec.require_positive_number(value, field="rows_per_second")


@pytest.mark.parametrize(
    "value",
    [
        "0" * 64,
        "sha256:" + "A" * 64,
        "SHA256:" + "a" * 64,
        "sha256:" + "a" * 63,
        True,
        None,
    ],
)
def test_digest_requires_tagged_lowercase_sha256(value: object) -> None:
    with pytest.raises(ValueError, match="tagged lowercase SHA-256"):
        codec.require_digest(value, field="manifest_sha256")
