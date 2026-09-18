from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/source-sink/mssql-to-clickhouse-cluster-full-refresh.yaml"
DOCS = (
    ROOT / "docs/clickhouse-cluster-publication.md",
    ROOT / "docs/clickhouse-cluster-publication-reference.md",
    ROOT / "docs/clickhouse-cluster-publication-runbook.md",
)
ERROR_DOCS = tuple(sorted((ROOT / "docs/errors").glob("CLICKHOUSE_CLUSTER_EXTERNAL_*.md")))


def test_external_cluster_full_refresh_example_parses_with_exact_mode() -> None:
    payload = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))

    assert payload["kind"] == "dpone.batch.v1"
    defaults = payload["defaults"]
    assert defaults["source"]["type"] == "mssql"
    assert payload["schemas"] == {"source_schema": {"tables": ["source_table"]}}

    sink = defaults["sink"]
    assert sink["table"] == {"schema": "analytics", "name": "target_table"}
    assert sink["staging"] == {"schema": "analytics"}
    assert sink["strategy"] == {"mode": "full_refresh", "max_source_bytes": 104857600}

    clickhouse = sink["options"]["physical_design"]["storage"]["clickhouse"]
    assert clickhouse["engine"] == "MergeTree"
    assert clickhouse["cluster"] == {
        "name": "analytics_cluster",
        "ddl_scope": "cluster",
        "replication_mode": "external",
    }


def test_external_publication_docs_cover_the_complete_user_journey() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in DOCS)

    required = (
        "## Prerequisites",
        "## First success with external replication",
        '"selected": "cluster_external"',
        '"replication_mode": "external"',
        '"no_fallback": true',
        "dpone.clickhouse.cluster-external-full-refresh-receipt.v1",
        "## Runtime admission",
        "## State and retry contract",
        "## Receipt and evidence contract",
        "## Safety rules",
        "## 2. Collect read-only evidence",
        "## 3. Classify the phase",
        "## 4. Retry safely",
        "## 5. Verify recovery",
        "Live certification remains **UNVERIFIED**",
    )
    for marker in required:
        assert marker in text

    assert "never falls back to local publication" in text
    assert "Do not issue another cluster exchange" in text
    assert "opaque member IDs" in text


def test_external_error_pages_are_actionable_and_cross_linked() -> None:
    expected = {
        "CLICKHOUSE_CLUSTER_EXTERNAL_MODE_MISMATCH",
        "CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_UNSUPPORTED",
        "CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INCOMPLETE",
        "CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED",
        "CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_DIVERGED",
        "CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL",
        "CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN",
    }
    assert {path.stem for path in ERROR_DOCS} == expected

    for path in ERROR_DOCS:
        text = path.read_text(encoding="utf-8")
        assert text.startswith(f"# {path.stem}\n")
        assert "clickhouse-cluster-publication" in text
        assert len(text.splitlines()) >= 8


def test_new_publication_material_uses_only_generic_identifiers() -> None:
    paths = (*DOCS, *ERROR_DOCS, EXAMPLE, Path(__file__))
    text = "\n".join(path.read_text(encoding="utf-8") for path in paths)

    assert "analytics_cluster" in text
    assert "source_table" in text
    assert "target_table" in text
    forbidden_absolute_paths = ("/" + "Users" + "/", "/" + "private" + "/")
    assert not any(marker in text for marker in forbidden_absolute_paths)
    assert "file" + "://" not in text
