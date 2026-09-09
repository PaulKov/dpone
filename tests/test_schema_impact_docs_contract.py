from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_schema_impact_is_documented_and_in_navigation() -> None:
    user_doc = (ROOT / "docs/schema-impact.md").read_text(encoding="utf-8")
    dev_doc = (ROOT / "docs/developer-schema-impact.md").read_text(encoding="utf-8")
    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (ROOT / "docs/index.md").read_text(encoding="utf-8")
    cli_reference = (ROOT / "docs/cli-reference.md").read_text(encoding="utf-8")
    architecture = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")

    for needle in (
        "compatibility_breaking",
        "data_destructive",
        "direct_rename",
        "shadow_cutover",
        "dpone schema impact plan",
        "dpone schema impact gate",
        "impact_plan_id",
        "dlt schema evolution",
        "Airbyte schema change management",
        "Fivetran",
        "Pentaho",
    ):
        assert needle in user_doc
    for needle in ("DependencyProvider", "SchemaImpactGate", "MigrationPackChangeExtractor"):
        assert needle in dev_doc
    assert "schema-impact.md" in nav
    assert "developer-schema-impact.md" in nav
    assert "Schema impact" in index
    assert "dpone schema impact plan" in cli_reference
    assert "dpone schema impact gate" in cli_reference
    assert "SchemaImpactGraph" in architecture
    assert "SchemaImpactGate" in architecture


def test_json_schemas_expose_schema_impact_contract() -> None:
    config_schema = json.loads((ROOT / "src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads((ROOT / "src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    assert "schema_impact" in json.dumps(config_schema)
    assert "schema_impact" in json.dumps(batch_schema)
    assert "compatibility_breaking" in json.dumps(config_schema)
    assert "shadow_cutover" in json.dumps(batch_schema)


def test_schema_impact_yaml_examples_are_valid() -> None:
    doc = (ROOT / "docs/schema-impact.md").read_text(encoding="utf-8")
    blocks = _yaml_blocks(doc)
    assert blocks
    for block in blocks:
        yaml.safe_load(block)


def _yaml_blocks(text: str) -> list[str]:
    blocks: list[str] = []
    inside = False
    current: list[str] = []
    for line in text.splitlines():
        if line.strip() == "```yaml":
            inside = True
            current = []
            continue
        if inside and line.strip() == "```":
            blocks.append("\n".join(current))
            inside = False
            continue
        if inside:
            current.append(line)
    return blocks
