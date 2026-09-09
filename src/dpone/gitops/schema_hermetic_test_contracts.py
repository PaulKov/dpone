"""Public JSON Schema contracts for hermetic authoring and CI reports."""

from __future__ import annotations

from typing import Any

from dpone.contracts.hermetic_test import MAX_EXPECTATION_ISSUES, MAX_SCHEMA_ASSERTIONS
from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_SHA256 = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
_TEXT = {"type": "string", "minLength": 1, "maxLength": 1024}
_RELATIVE_PATH = {
    "type": "string",
    "minLength": 1,
    "maxLength": 1024,
    "not": {"pattern": r"^(?:/|[A-Za-z]:|.*\\)"},
}
_SCHEMA_TYPE = {"enum": ["null", "bool", "int64", "float64", "string", "array", "object", "mixed"]}
_TEST_REPORT_REQUIRED = (
    "schema",
    "test_id",
    "name",
    "status",
    "execution_status",
    "data_outcome",
    "coverage",
    "pipeline",
    "process",
    "fixtures",
    "temporary_target",
    "expectations",
    "errors",
    "duration_ms",
)


def hermetic_test_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    """Return the authoring, individual-report, and suite-report contracts."""

    return (_test_manifest_contract(), _test_report_contract(), _test_suite_report_contract())


def _test_manifest_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="test-manifest",
        kind="dpone.test.v1",
        title="dpone GitOps hermetic test manifest",
        required=("kind", "pipeline", "input", "expect"),
        properties={
            "kind": {"const": "dpone.test.v1"},
            "name": {"type": "string", "minLength": 1, "maxLength": 128},
            "pipeline": _RELATIVE_PATH,
            "process": {"type": "string", "minLength": 1, "maxLength": 256},
            "input": {"$ref": "#/$defs/input"},
            "expect": {"$ref": "#/$defs/expect"},
            "limits": {"$ref": "#/$defs/limits"},
        },
        defs={
            "input": {
                "type": "object",
                "required": ["fixture"],
                "additionalProperties": False,
                "properties": {
                    "fixture": _RELATIVE_PATH,
                    "format": {"const": "jsonl", "default": "jsonl"},
                    "initial_target": {
                        "type": "object",
                        "required": ["fixture"],
                        "additionalProperties": False,
                        "properties": {"fixture": _RELATIVE_PATH},
                    },
                },
            },
            "expect": {
                "type": "object",
                "required": ["rows"],
                "additionalProperties": False,
                "properties": {
                    "rows": {"type": "integer", "minimum": 0, "maximum": 20000},
                    "rejected_rows": {"const": 0, "default": 0},
                    "schema": {
                        "type": "object",
                        "maxProperties": MAX_SCHEMA_ASSERTIONS,
                        "additionalProperties": _SCHEMA_TYPE,
                    },
                    "output_fixture": _RELATIVE_PATH,
                    "match": {"const": "exact_unordered"},
                },
                "dependentRequired": {"match": ["output_fixture"]},
            },
            "limits": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "max_bytes": {"type": "integer", "minimum": 1, "maximum": 10485760, "default": 10485760},
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 10000},
                    "max_line_bytes": {"type": "integer", "minimum": 1, "maximum": 1048576, "default": 1048576},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300, "default": 30},
                },
            },
        },
        additional_properties=False,
    )


def _test_report_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="test-report",
        kind="dpone.test-report.v1",
        title="dpone GitOps hermetic test report",
        required=_TEST_REPORT_REQUIRED,
        properties=_test_report_properties(),
        defs=_report_defs(),
        additional_properties=False,
    )


def _test_suite_report_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="test-suite-report",
        kind="dpone.test-suite-report.v1",
        title="dpone GitOps hermetic test suite report",
        required=("schema", "passed", "status", "counts", "tests", "duration_ms"),
        properties={
            "schema": {"const": "dpone.test-suite-report.v1"},
            "passed": {"type": "boolean"},
            "status": {"enum": ["passed", "failed", "blocked"]},
            "counts": {
                "type": "object",
                "required": ["total", "passed", "failed", "blocked"],
                "additionalProperties": False,
                "properties": {
                    name: {"type": "integer", "minimum": 0} for name in ("total", "passed", "failed", "blocked")
                },
            },
            "tests": {"type": "array", "maxItems": 100, "items": {"$ref": "#/$defs/test_report"}},
            "duration_ms": {"type": "integer", "minimum": 0},
        },
        defs={**_report_defs(), "test_report": _test_report_object_schema()},
        additional_properties=False,
    )


def _test_report_object_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": list(_TEST_REPORT_REQUIRED),
        "additionalProperties": False,
        "properties": _test_report_properties(),
    }


def _test_report_properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.test-report.v1"},
        "test_id": _SHA256,
        "name": {"type": "string", "minLength": 1, "maxLength": 128},
        "status": {"enum": ["passed", "failed", "blocked"]},
        "execution_status": {"enum": ["succeeded", "failed", "skipped"]},
        "data_outcome": {"enum": ["passed", "failed_quality_gate", "unknown"]},
        "coverage": {"$ref": "#/$defs/coverage"},
        "pipeline": {"$ref": "#/$defs/pipeline"},
        "process": {"type": "object", "additionalProperties": True},
        "fixtures": {
            "type": "object",
            "additionalProperties": {"$ref": "#/$defs/fixture"},
            "maxProperties": 3,
        },
        "temporary_target": {"$ref": "#/$defs/temporary_target"},
        "expectations": {
            "type": "array",
            "maxItems": MAX_EXPECTATION_ISSUES,
            "items": {"$ref": "#/$defs/issue"},
        },
        "errors": {"type": "array", "maxItems": 1000, "items": {"$ref": "#/$defs/error"}},
        "duration_ms": {"type": "integer", "minimum": 0},
    }


def _report_defs() -> dict[str, Any]:
    nullable_sha = {"oneOf": [_SHA256, {"type": "null"}]}
    return {
        "coverage": {
            "type": "object",
            "required": ["level", "includes", "excludes"],
            "additionalProperties": False,
            "properties": {
                "level": {"const": "hermetic_contract"},
                "includes": {"type": "array", "items": _TEXT},
                "excludes": {"type": "array", "items": _TEXT},
            },
        },
        "pipeline": {
            "type": "object",
            "required": ["path", "semantic_fingerprint"],
            "additionalProperties": False,
            "properties": {"path": {"type": "string"}, "semantic_fingerprint": nullable_sha},
        },
        "fixture": {
            "type": "object",
            "required": ["path", "sha256", "bytes", "rows"],
            "additionalProperties": False,
            "properties": {
                "path": _RELATIVE_PATH,
                "sha256": _SHA256,
                "bytes": {"type": "integer", "minimum": 0},
                "rows": {"type": "integer", "minimum": 0},
            },
        },
        "temporary_target": {
            "type": "object",
            "required": ["uri", "rows", "rejected_rows", "schema"],
            "additionalProperties": False,
            "properties": {
                "uri": {
                    "oneOf": [
                        {"type": "string", "pattern": "^tmp://dpone-tests/sha256-[0-9a-f]{64}$"},
                        {"type": "null"},
                    ]
                },
                "rows": {"type": "integer", "minimum": 0},
                "rejected_rows": {"const": 0},
                "schema": {"type": "object", "additionalProperties": _SCHEMA_TYPE},
            },
        },
        "issue": {
            "type": "object",
            "required": ["code", "path", "message"],
            "additionalProperties": True,
            "properties": {"code": _TEXT, "path": _TEXT, "message": _TEXT},
        },
        "error": {
            "type": "object",
            "required": ["schema", "code", "stage", "severity", "message", "fixes", "docs_url"],
            "additionalProperties": True,
            "properties": {
                "schema": {"const": "dpone.error.v1"},
                "code": _TEXT,
                "stage": _TEXT,
                "severity": {"const": "error"},
                "message": _TEXT,
                "fixes": {"type": "array"},
                "docs_url": _TEXT,
            },
        },
    }


__all__ = ["hermetic_test_schema_contracts"]
