"""Bounded parser and JSONL codec for ``dpone.test.v1`` authoring."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.hermetic_test import (
    HERMETIC_TEST_KIND,
    MAX_SCHEMA_ASSERTIONS,
    SCHEMA_TYPES,
    HermeticExpectationContract,
    HermeticFixture,
    HermeticInputContract,
    HermeticTestContract,
    HermeticTestError,
    HermeticTestLimits,
    hermetic_execution_plan,
    hermetic_execution_supported,
)
from dpone.manifest.authoring import AuthoringCompilation, AuthoringCompiler, default_authoring_compiler
from dpone.manifest.errors import ManifestConfigurationError

HARD_MAX_BYTES = 10 * 1024 * 1024
HARD_MAX_ROWS = 10_000
HARD_MAX_LINE_BYTES = 1024 * 1024
HARD_MAX_TIMEOUT_SECONDS = 300
HARD_MAX_JSON_DEPTH = 64
_ROOT_KEYS = {"kind", "name", "pipeline", "process", "input", "expect", "limits"}
_INPUT_KEYS = {"fixture", "format", "initial_target"}
_EXPECT_KEYS = {"rows", "rejected_rows", "schema", "output_fixture", "match"}
_LIMIT_KEYS = {"max_bytes", "max_rows", "max_line_bytes", "timeout_seconds"}


class _DuplicateJsonKey(ValueError):
    pass


def parse_hermetic_test_contract(payload: object, *, default_name: str) -> HermeticTestContract:
    """Validate one mapping and return the normalized immutable contract."""

    root = _mapping(payload, "test manifest")
    _known_keys(root, _ROOT_KEYS, "test manifest")
    if root.get("kind") != HERMETIC_TEST_KIND:
        raise _invalid("kind must be dpone.test.v1.")
    name = _text(root.get("name", default_name), "name", max_length=128)
    pipeline = _text(root.get("pipeline"), "pipeline", max_length=1024)
    process = _optional_field_text(root, "process", "process", max_length=256)
    input_contract = _parse_input(root.get("input"))
    expectations = _parse_expectations(root.get("expect"))
    limits = _parse_limits(root["limits"]) if "limits" in root else _parse_limits({})
    normalized = {
        "kind": HERMETIC_TEST_KIND,
        "name": name,
        "pipeline": pipeline,
        "process": process,
        "input": {
            "fixture": input_contract.fixture,
            "format": input_contract.format,
            "initial_target": (
                {"fixture": input_contract.initial_target_fixture}
                if input_contract.initial_target_fixture is not None
                else None
            ),
        },
        "expect": {
            "rows": expectations.rows,
            "rejected_rows": expectations.rejected_rows,
            "schema": expectations.schema_mapping(),
            "output_fixture": expectations.output_fixture,
            "match": expectations.match,
        },
        "limits": {
            "max_bytes": limits.max_bytes,
            "max_rows": limits.max_rows,
            "max_line_bytes": limits.max_line_bytes,
            "timeout_seconds": limits.timeout_seconds,
        },
    }
    return HermeticTestContract(
        name=name,
        pipeline=pipeline,
        process=process,
        input=input_contract,
        expect=expectations,
        limits=limits,
        normalized_payload=normalized,
    )


def load_jsonl_fixture(content: bytes, *, path: str, limits: HermeticTestLimits) -> HermeticFixture:
    """Parse rows from the exact bytes used for the fixture fingerprint."""

    if len(content) > limits.max_bytes:
        raise HermeticTestError(
            "DPONE_TEST_FIXTURE_LIMIT_EXCEEDED",
            "Fixture exceeds the configured byte limit.",
            exit_code=4,
            stage="test_fixture",
            path=path,
        )
    rows: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        if len(raw_line) > limits.max_line_bytes:
            raise HermeticTestError(
                "DPONE_TEST_FIXTURE_LIMIT_EXCEEDED",
                f"Fixture line {line_number} exceeds the configured line limit.",
                exit_code=4,
                stage="test_fixture",
                path=path,
            )
        if not raw_line.strip():
            raise _fixture_invalid("Fixture contains a blank JSONL record.", path)
        if len(rows) >= limits.max_rows:
            raise HermeticTestError(
                "DPONE_TEST_FIXTURE_LIMIT_EXCEEDED",
                "Fixture exceeds the configured row limit.",
                exit_code=4,
                stage="test_fixture",
                path=path,
            )
        rows.append(_parse_json_row(raw_line, path=path, line_number=line_number))
    return HermeticFixture(
        path=path,
        sha256="sha256:" + hashlib.sha256(content).hexdigest(),
        byte_count=len(content),
        rows=tuple(rows),
    )


def compile_hermetic_pipeline(
    payload: Mapping[str, Any],
    *,
    source_path: Path,
    project_root: Path,
    compiler: AuthoringCompiler | None = None,
) -> AuthoringCompilation:
    """Compile one authoring source and normalize compiler failures safely."""

    try:
        return (compiler or default_authoring_compiler()).compile(
            payload,
            source_path=source_path,
            project_root=project_root,
            include_content_dependencies=False,
        )
    except ManifestConfigurationError as exc:
        raw_code = str(getattr(exc, "code", "DPONE_TEST_PIPELINE_INVALID"))
        code = raw_code if raw_code.startswith("DPONE_") else "DPONE_TEST_PIPELINE_INVALID"
        raise HermeticTestError(
            code,
            "Pipeline source could not be compiled for hermetic testing.",
            stage="test_compile",
            path=source_path.relative_to(project_root).as_posix(),
        ) from exc


def _parse_input(raw: object) -> HermeticInputContract:
    payload = _mapping(raw, "input")
    _known_keys(payload, _INPUT_KEYS, "input")
    fixture = _text(payload.get("fixture"), "input.fixture", max_length=1024)
    format_name = _text(payload.get("format", "jsonl"), "input.format", max_length=32)
    if format_name != "jsonl":
        raise _invalid("input.format must be jsonl.")
    initial_fixture: str | None = None
    if "initial_target" in payload:
        initial = _mapping(payload["initial_target"], "input.initial_target")
        _known_keys(initial, {"fixture"}, "input.initial_target")
        initial_fixture = _text(initial.get("fixture"), "input.initial_target.fixture", max_length=1024)
    return HermeticInputContract(fixture=fixture, initial_target_fixture=initial_fixture, format=format_name)


def _parse_expectations(raw: object) -> HermeticExpectationContract:
    payload = _mapping(raw, "expect")
    _known_keys(payload, _EXPECT_KEYS, "expect")
    rows = _bounded_int(payload.get("rows"), "expect.rows", minimum=0, maximum=HARD_MAX_ROWS * 2)
    rejected = _bounded_int(payload.get("rejected_rows", 0), "expect.rejected_rows", minimum=0, maximum=0)
    raw_schema = payload.get("schema", {})
    schema_payload = _mapping(raw_schema, "expect.schema")
    if len(schema_payload) > MAX_SCHEMA_ASSERTIONS:
        raise _invalid(f"expect.schema supports at most {MAX_SCHEMA_ASSERTIONS} columns.")
    schema: list[tuple[str, str]] = []
    for raw_name, raw_type in schema_payload.items():
        name = _text(raw_name, "expect.schema column", max_length=256)
        type_name = _text(raw_type, f"expect.schema.{name}", max_length=32)
        if type_name not in SCHEMA_TYPES:
            raise _invalid(f"expect.schema.{name} uses an unsupported logical type.")
        schema.append((name, type_name))
    output_fixture = _optional_field_text(payload, "output_fixture", "expect.output_fixture", max_length=1024)
    match = _optional_field_text(payload, "match", "expect.match", max_length=32)
    if output_fixture is not None:
        match = match or "exact_unordered"
        if match != "exact_unordered":
            raise _invalid("expect.match must be exact_unordered in v1.")
    elif match is not None:
        raise _invalid("expect.match requires expect.output_fixture.")
    return HermeticExpectationContract(
        rows=rows,
        rejected_rows=rejected,
        schema=tuple(sorted(schema)),
        output_fixture=output_fixture,
        match=match,
    )


def _parse_limits(raw: object) -> HermeticTestLimits:
    payload = _mapping(raw, "limits")
    _known_keys(payload, _LIMIT_KEYS, "limits")
    return HermeticTestLimits(
        max_bytes=_bounded_int(payload.get("max_bytes", HARD_MAX_BYTES), "limits.max_bytes", 1, HARD_MAX_BYTES),
        max_rows=_bounded_int(payload.get("max_rows", HARD_MAX_ROWS), "limits.max_rows", 1, HARD_MAX_ROWS),
        max_line_bytes=_bounded_int(
            payload.get("max_line_bytes", HARD_MAX_LINE_BYTES),
            "limits.max_line_bytes",
            1,
            HARD_MAX_LINE_BYTES,
        ),
        timeout_seconds=_bounded_int(
            payload.get("timeout_seconds", 30),
            "limits.timeout_seconds",
            1,
            HARD_MAX_TIMEOUT_SECONDS,
        ),
    )


def _parse_json_row(raw: bytes, *, path: str, line_number: int) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
        row = json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey, ValueError, RecursionError) as exc:
        raise _fixture_invalid(f"Fixture row {line_number} is not a valid unique-key JSON object.", path) from exc
    if not isinstance(row, dict):
        raise _fixture_invalid(f"Fixture row {line_number} must be a JSON object.", path)
    try:
        _validate_json_value(row)
    except ValueError as exc:
        raise _fixture_invalid(
            f"Fixture row {line_number} contains an unsupported or excessively nested JSON value.",
            path,
        ) from exc
    return row


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(value)


def _validate_json_value(value: Any) -> None:
    pending: list[tuple[Any, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, bool | str) or item is None:
            continue
        if isinstance(item, int):
            if item < -(2**63) or item > 2**63 - 1:
                raise ValueError("integer outside int64")
            continue
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("non-finite number")
            continue
        if isinstance(item, list | dict):
            if depth >= HARD_MAX_JSON_DEPTH:
                raise ValueError("JSON nesting exceeds the hard depth limit")
            values = item if isinstance(item, list) else item.values()
            pending.extend((child, depth + 1) for child in values)
            continue
        raise ValueError("unsupported JSON value")


def _mapping(raw: object, label: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or not all(isinstance(key, str) for key in raw):
        raise _invalid(f"{label} must be an object with string keys.")
    return dict(raw)


def _known_keys(payload: Mapping[str, Any], allowed: set[str], label: str) -> None:
    unknown_count = len(set(payload) - allowed)
    if unknown_count:
        raise _invalid(f"{label} contains {unknown_count} unsupported field(s).")


def _text(raw: object, label: str, *, max_length: int) -> str:
    if not isinstance(raw, str) or not raw.strip() or len(raw.strip()) > max_length:
        raise _invalid(f"{label} must be a non-empty bounded string.")
    return raw.strip()


def _optional_field_text(
    payload: Mapping[str, Any],
    key: str,
    label: str,
    *,
    max_length: int,
) -> str | None:
    if key not in payload:
        return None
    return _text(payload[key], label, max_length=max_length)


def _bounded_int(raw: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < minimum or raw > maximum:
        raise _invalid(f"{label} must be an integer between {minimum} and {maximum}.")
    return raw


def _invalid(message: str) -> HermeticTestError:
    return HermeticTestError("DPONE_TEST_MANIFEST_INVALID", message, stage="test_manifest")


def _fixture_invalid(message: str, path: str) -> HermeticTestError:
    return HermeticTestError("DPONE_TEST_FIXTURE_INVALID", message, stage="test_fixture", path=path)


__all__ = [
    "HARD_MAX_BYTES",
    "HARD_MAX_LINE_BYTES",
    "HARD_MAX_ROWS",
    "HARD_MAX_TIMEOUT_SECONDS",
    "AuthoringCompiler",
    "compile_hermetic_pipeline",
    "hermetic_execution_plan",
    "hermetic_execution_supported",
    "load_jsonl_fixture",
    "parse_hermetic_test_contract",
]
