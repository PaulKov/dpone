from __future__ import annotations

import json
from pathlib import Path


def test_manifest_schemas_expose_schema_identity_contract() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        identity = sink_options["schema_identity"]["properties"]
        assert identity["enabled"]["default"] is False
        assert identity["rename"]["properties"]["strategy"]["enum"] == [
            "expand_contract",
            "runtime_alias",
            "direct_rename",
        ]
        assert identity["columns"]["additionalProperties"]["properties"]["aliases"]["items"]["properties"][
            "compatibility"
        ]["enum"] == ["read_alias", "dual_write"]


def test_schema_identity_docs_cover_taxonomy_and_rename_vs_variant_columns() -> None:
    user_doc = Path("docs/schema-identity.md").read_text(encoding="utf-8")
    dev_doc = Path("docs/developer-schema-identity.md").read_text(encoding="utf-8")
    migration_doc = Path("docs/schema-migration-control.md").read_text(encoding="utf-8")
    evolution_doc = Path("docs/schema-evolution.md").read_text(encoding="utf-8")

    for term in (
        "SchemaIdentityResolver",
        "AliasProjectionPlanner",
        "rename_alias",
        "__dpone__nc__<canonical_column>",
        "direct_rename",
        "downstream",
    ):
        assert term in user_doc
    assert "TargetIdentityMigrationDialect" in dev_doc
    assert "schema identity" in migration_doc.lower()
    assert "Schema Identity" in evolution_doc
