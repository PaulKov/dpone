from __future__ import annotations

import json
from pathlib import Path

import yaml
from jsonschema import Draft7Validator


def test_public_schema_documents_backfill_advisor_and_state_options() -> None:
    schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    contract = schema["properties"]["sink"]["properties"]["strategy"]["properties"]["backfill"]
    backfill = contract["properties"]

    assert contract["additionalProperties"] is False
    assert backfill["inner_mode"]["enum"] == [
        "partition_replace",
        "replace",
        "incremental_append",
        "incremental_merge",
    ]
    assert backfill["parallel_workers"]["maximum"] == 256
    assert backfill["max_chunks"]["maximum"] == 1_000_000
    assert backfill["retry_policy"]["enum"] == ["non_committed", "failed_only"]
    assert backfill["lease_ttl_minutes"]["minimum"] == 1
    assert backfill["lease_ttl_minutes"]["maximum"] == 10_080
    assert backfill["predicate_dialect"]["enum"] == [
        "generic",
        "clickhouse",
        "mssql",
        "postgres",
        "postgresql",
    ]
    assert backfill["state"]["properties"]["backend"]["enum"] == ["local_file", "audit_schema"]
    assert backfill["state"]["properties"]["require_distributed_lock"]["default"] is False
    assert backfill["publication"]["additionalProperties"] is False
    assert backfill["publication"]["properties"]["mode"]["enum"] == ["direct", "shadow_swap"]
    assert backfill["publication"]["properties"]["retain_backup"]["default"] is True
    assert backfill["publication"]["properties"]["artifact_scope"] == {
        "type": "string",
        "description": (
            "Use legacy stable artifact names or deterministic campaign-scoped names so a reviewed new "
            "backfill_id can coexist with retained evidence from an older campaign."
        ),
        "enum": ["stable", "campaign"],
        "default": "stable",
    }
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    batch_publication = batch_schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["strategy"][
        "properties"
    ]["backfill"]["properties"]["publication"]
    assert batch_publication["properties"]["artifact_scope"] == backfill["publication"]["properties"]["artifact_scope"]
    assert backfill["advisor"]["properties"]["optimize_for"]["enum"] == [
        "balanced",
        "speed",
        "source_safety",
        "worker_safety",
    ]
    chunk = backfill["chunk"]
    assert chunk["properties"]["kind"]["enum"] == ["date", "timestamp", "integer", "uuid"]
    assert chunk["properties"]["buckets"]["minimum"] == 1
    assert any(branch.get("properties", {}).get("kind") == {"const": "uuid"} for branch in chunk["oneOf"])


def test_public_schemas_and_examples_expose_two_explicit_xmin_phases() -> None:
    paths = (
        Path("src/dpone/schema/etl-config.schema.json"),
        Path("src/dpone/schema/etl-batch-manifest.schema.json"),
    )
    for path in paths:
        schema = json.loads(path.read_text(encoding="utf-8"))
        if path.name == "etl-config.schema.json":
            source_options = schema["properties"]["source"]["properties"]["options"]["properties"]
        else:
            source_options = schema["definitions"]["process_fragment"]["properties"]["source"]["properties"]["options"][
                "properties"
            ]
        contract = source_options["xmin_execution"]
        assert contract["additionalProperties"] is False
        assert contract["required"] == ["mode", "handoff_id"]
        assert contract["properties"]["mode"]["enum"] == ["initial", "incremental"]

    batch_schema = json.loads(paths[1].read_text(encoding="utf-8"))
    validator = Draft7Validator(batch_schema)
    for phase in ("initial", "incremental"):
        example = Path(f"examples/batch/postgres-xmin-{phase}-to-mssql.batch.yaml")
        authored = yaml.safe_load(example.read_text(encoding="utf-8"))
        assert not list(validator.iter_errors(authored))
        policy = authored["defaults"]["source"]["options"]["xmin_execution"]
        assert policy == {"mode": phase, "handoff_id": "orders_v1"}
