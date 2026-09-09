from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft7Validator

ROOT = Path(__file__).resolve().parents[1]
LOAD_GOVERNANCE_DOC = ROOT / "docs/load-governance.md"
SCHEMA_PATHS = {
    "batch": ROOT / "src/dpone/schema/etl-batch-manifest.schema.json",
    "flow": ROOT / "src/dpone/schema/etl-flow-manifest.schema.json",
}
ACCEPTANCE_DEFINITIONS = (
    "quality_acceptance",
    "quality_acceptance_column_selector",
    "quality_acceptance_enabled_column_selector",
)


def _schema(schema_name: str) -> dict[str, Any]:
    return json.loads(SCHEMA_PATHS[schema_name].read_text(encoding="utf-8"))


def _manifest(schema_name: str, acceptance: object) -> dict[str, Any]:
    quality = {"acceptance": acceptance}
    if schema_name == "batch":
        return {
            "kind": "dpone.batch.v1",
            "quality": quality,
            "schemas": {"dbo": {"tables": ["orders"]}},
        }
    return {
        "kind": "dpone.flow.v1",
        "authoring": {
            "mode": "flow",
            "source": "pipelines/orders/pipeline.yaml",
        },
        "metadata": {"id": "orders", "domain": "sales"},
        "quality": quality,
        "processes": [
            {
                "name": "orders",
                "source": {
                    "type": "mssql",
                    "connection_ref": "source",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "sink",
                    "table": {"schema": "raw", "name": "orders"},
                    "strategy": {"mode": "full_refresh"},
                },
            }
        ],
    }


def _errors(schema_name: str, acceptance: object) -> list[Any]:
    validator = Draft7Validator(_schema(schema_name))
    return list(validator.iter_errors(_manifest(schema_name, acceptance)))


def _quality_errors(
    schema_name: str,
    quality: dict[str, object],
    *,
    process_level: bool,
) -> list[Any]:
    manifest = _manifest(schema_name, {"enabled": False})
    if process_level:
        manifest.pop("quality")
        if schema_name == "batch":
            manifest["defaults"] = {"quality": quality}
        else:
            processes = manifest["processes"]
            assert isinstance(processes, list)
            process = processes[0]
            assert isinstance(process, dict)
            process["quality"] = quality
    else:
        manifest["quality"] = quality
    return list(Draft7Validator(_schema(schema_name)).iter_errors(manifest))


def _documented_acceptance_examples() -> list[dict[str, Any]]:
    content = LOAD_GOVERNANCE_DOC.read_text(encoding="utf-8")
    examples: list[dict[str, Any]] = []
    for block in re.findall(r"```yaml\n(.*?)```", content, flags=re.DOTALL):
        value = yaml.safe_load(block)
        if not isinstance(value, Mapping):
            continue
        quality = value.get("quality")
        acceptance = quality.get("acceptance") if isinstance(quality, Mapping) else None
        if isinstance(acceptance, Mapping):
            examples.append(dict(acceptance))
    return examples


def test_batch_and_flow_expose_identical_quality_acceptance_contracts() -> None:
    batch = _schema("batch")
    flow = _schema("flow")

    Draft7Validator.check_schema(batch)
    Draft7Validator.check_schema(flow)
    for definition in ACCEPTANCE_DEFINITIONS:
        assert batch["definitions"][definition] == flow["definitions"][definition]

    expected_acceptance_ref = {"$ref": "#/definitions/quality_acceptance"}
    expected_quality_ref = {"$ref": "#/definitions/managed_quality"}
    for schema in (batch, flow):
        assert schema["properties"]["quality"] == expected_quality_ref
        assert schema["definitions"]["managed_quality"]["properties"]["acceptance"] == expected_acceptance_ref
    assert batch["definitions"]["process_fragment"]["properties"]["quality"] == expected_quality_ref
    assert flow["definitions"]["process"]["properties"]["quality"] == expected_quality_ref


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
@pytest.mark.parametrize(
    "acceptance",
    [
        pytest.param(
            {"enabled": True},
            id="enabled-with-defaults",
        ),
        pytest.param(
            {"enabled": True, "capture": {}, "checks": {}},
            id="enabled-with-explicit-default-objects",
        ),
        pytest.param(
            {
                "enabled": True,
                "mode": "required",
                "capture": {"source": True, "staged": True, "target": True},
                "checks": {
                    "row_count": True,
                    "null_counts": ["project_id", "item_id"],
                    "distinct_counts": "business_columns",
                },
            },
            id="staged-required",
        ),
        pytest.param(
            {
                "enabled": True,
                "mode": "required",
                "capture": {"source": True, "staged": False, "target": True},
                "checks": {
                    "row_count": True,
                    "null_counts": "source_and_binary_semantics",
                    "distinct_counts": ["project_id"],
                },
            },
            id="legacy-required",
        ),
        pytest.param(
            {
                "enabled": False,
                "capture": {"source": False, "staged": False, "target": False},
                "checks": {
                    "row_count": False,
                    "null_counts": "off",
                    "distinct_counts": "none",
                },
            },
            id="disabled-shaped-policy",
        ),
    ],
)
def test_official_schemas_accept_supported_acceptance_examples(
    schema_name: str,
    acceptance: dict[str, Any],
) -> None:
    assert _errors(schema_name, acceptance) == []


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
@pytest.mark.parametrize(
    "selector",
    [
        False,
        True,
        "off",
        "none",
        "all_columns",
        "business_columns",
        "source_and_binary_semantics",
        "strict",
        ["project_id", "item_id"],
    ],
)
@pytest.mark.parametrize("check_name", ["null_counts", "distinct_counts"])
def test_official_schemas_accept_every_supported_column_selector(
    schema_name: str,
    selector: bool | str | list[str],
    check_name: str,
) -> None:
    acceptance = {
        "enabled": True,
        "checks": {"row_count": True, check_name: selector},
    }

    assert _errors(schema_name, acceptance) == []


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
@pytest.mark.parametrize(
    ("case", "acceptance"),
    [
        ("acceptance-not-object", "enabled"),
        ("missing-enabled", {}),
        ("invalid-enabled", {"enabled": "true"}),
        ("misspelled-mode", {"enabled": True, "mode": "require"}),
        ("unknown-acceptance-key", {"enabled": True, "unexpected": True}),
        ("capture-not-object", {"enabled": True, "capture": ["source"]}),
        ("unknown-capture-key", {"enabled": True, "capture": {"destination": True}}),
        ("invalid-capture-boolean", {"enabled": True, "capture": {"source": 1}}),
        ("checks-not-object", {"enabled": True, "checks": ["row_count"]}),
        ("unknown-check-key", {"enabled": True, "checks": {"rows": True}}),
        ("invalid-row-count-boolean", {"enabled": True, "checks": {"row_count": "exact"}}),
        ("unknown-null-selector", {"enabled": True, "checks": {"null_counts": "everything"}}),
        ("empty-distinct-selector", {"enabled": True, "checks": {"distinct_counts": []}}),
        (
            "duplicate-null-selector",
            {"enabled": True, "checks": {"null_counts": ["project_id", "project_id"]}},
        ),
        ("empty-column-name", {"enabled": True, "checks": {"distinct_counts": [""]}}),
        (
            "no-requested-side",
            {
                "enabled": True,
                "capture": {"source": False, "staged": False, "target": False},
            },
        ),
        (
            "no-requested-check",
            {
                "enabled": True,
                "checks": {
                    "row_count": False,
                    "null_counts": "off",
                    "distinct_counts": False,
                },
            },
        ),
    ],
)
def test_official_schemas_reject_malformed_acceptance_authoring(
    schema_name: str,
    case: str,
    acceptance: object,
) -> None:
    assert _errors(schema_name, acceptance), case


@pytest.mark.parametrize("schema_name", SCHEMA_PATHS)
@pytest.mark.parametrize("process_level", [False, True], ids=["top-level", "process-level"])
@pytest.mark.parametrize("misspelled_key", ["acceptence", "accepance", "acceptancee"])
def test_official_schemas_reject_misspelled_quality_acceptance_key(
    schema_name: str,
    process_level: bool,
    misspelled_key: str,
) -> None:
    errors = _quality_errors(
        schema_name,
        {misspelled_key: {"enabled": True}},
        process_level=process_level,
    )

    assert errors
    assert any(error.validator == "not" for error in errors)


def test_load_governance_acceptance_examples_validate_against_both_schemas() -> None:
    examples = _documented_acceptance_examples()

    assert len(examples) >= 2
    for acceptance in examples:
        for schema_name in SCHEMA_PATHS:
            assert _errors(schema_name, acceptance) == []


def test_load_governance_documents_physical_sides_and_staged_lifecycle_order() -> None:
    content = LOAD_GOVERNANCE_DOC.read_text(encoding="utf-8")
    section = content.split("### Acceptance lifecycle and physical sides", 1)[1].split(
        "## Airflow GitOps UX",
        1,
    )[0]

    for token in (
        "legacy/non-staged",
        "native resume-only",
        "`staged` is physically unavailable",
        "`capture.staged: false`",
        "`post_commit`",
        "`resume_validation`",
    ):
        assert token in section

    ordered_steps = (
        "Capture requested source and staged metrics.",
        "Finalize the target; this is the target commit boundary.",
        "Capture requested target metrics.",
        "Cleanup staging artifacts.",
        "Persist source state",
        "Execute `post_hook`",
    )
    positions = [section.index(step) for step in ordered_steps]
    assert positions == sorted(positions)
