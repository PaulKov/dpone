from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
SOURCE_SINK_DOCS = sorted((DOCS / "source-sink").glob("*.md"))

REQUIRED_SOURCE_SINK_SECTIONS = (
    "## Copy/paste manifest",
    "## Supported load strategies",
    "## Runtime algorithm",
    "## Strategy behavior",
    "## Schema evolution and type mapping",
    "## Runbook",
    "## Cross-links",
)

REQUIRED_SOURCE_SINK_LINKS = (
    "../load-strategies.md",
    "../schema-evolution.md",
    "../type-mapping-matrix.md",
    "../source-sink-matrix.md",
)

REQUIRED_LOAD_STRATEGIES = (
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "replace",
    "partition_replace",
    "snapshot_diff",
    "scd2",
    "cdc_apply",
    "backfill",
)

REQUIRED_SOURCE_CAPABILITIES = (
    "xmin",
    "cdc",
    "snapshot_reconciliation",
)


def test_each_source_sink_guide_is_copy_paste_ready() -> None:
    offenders: list[str] = []
    assert SOURCE_SINK_DOCS, "Expected source/sink guide files"

    for path in SOURCE_SINK_DOCS:
        text = path.read_text(encoding="utf-8")
        missing_sections = [section for section in REQUIRED_SOURCE_SINK_SECTIONS if section not in text]
        missing_links = [link for link in REQUIRED_SOURCE_SINK_LINKS if link not in text]
        if "```yaml" not in text:
            missing_sections.append("```yaml")
        if "```mermaid" not in text:
            missing_sections.append("```mermaid")
        if "flowchart" not in text and "sequenceDiagram" not in text:
            missing_sections.append("flowchart or sequenceDiagram")
        if missing_sections or missing_links:
            offenders.append(f"{path.relative_to(ROOT)} missing sections={missing_sections} links={missing_links}")

    assert offenders == []


def test_load_strategy_docs_are_navigable_and_diagrammed() -> None:
    text = (DOCS / "load-strategies.md").read_text(encoding="utf-8")
    offenders: list[str] = []

    for heading in ("## Table of contents", "## Strategy support matrix", "## Cross-links"):
        if heading not in text:
            offenders.append(f"missing {heading}")
    if "source-sink/" not in text:
        offenders.append("missing source/sink cross-link")

    for strategy in REQUIRED_LOAD_STRATEGIES:
        if f"## `{strategy}`" not in text:
            offenders.append(f"missing strategy section {strategy}")
        section = text.split(f"## `{strategy}`", 1)[-1].split("\n## `", 1)[0]
        if "```yaml" not in section:
            offenders.append(f"missing yaml example for {strategy}")
        if "```mermaid" not in section:
            offenders.append(f"missing mermaid diagram for {strategy}")

    for capability in REQUIRED_SOURCE_CAPABILITIES:
        if f"## `{capability}`" not in text:
            offenders.append(f"missing source/capability section {capability}")

    assert offenders == []


def test_source_sink_matrix_links_every_copy_paste_guide() -> None:
    matrix = (DOCS / "source-sink-matrix.md").read_text(encoding="utf-8")
    missing = [f"source-sink/{path.name}" for path in SOURCE_SINK_DOCS if f"source-sink/{path.name}" not in matrix]

    assert missing == []


def test_clickhouse_to_mssql_documents_cloud_http_transport() -> None:
    text = (DOCS / "source-sink" / "clickhouse-to-mssql.md").read_text(encoding="utf-8")

    for expected in (
        "## ClickHouse connection transport",
        "clickhouse-connect",
        "interface",
        "port: 8443",
        "Unexpected packet",
    ):
        assert expected in text
