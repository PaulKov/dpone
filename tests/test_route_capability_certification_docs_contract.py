from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_route_capability_certification_docs_cover_cli_artifacts_and_quality_gates() -> None:
    text = _read(DOCS / "route-capability-certification.md")

    for expected in (
        "dpone certify route-capabilities",
        "preflight_only",
        "small_live",
        "work-item_account_sales_benchmark",
        "certification.json",
        "route_decisions.jsonl",
        "quality_report.json",
        "direct_push_columnar",
        "Direct columnar push",
        "source count = target count",
        "NULL counts",
        "distinct counts",
        "__dpone__run_id",
        "runtime_access.connection_id",
        "named_collection: dpone_stage",
    ):
        assert expected in text


def test_route_capability_certification_docs_include_product_links_and_troubleshooting() -> None:
    text = _read(DOCS / "route-capability-certification.md")

    for expected in (
        "https://supermetal.io/blog/sql-server-clickhouse-benchmark",
        "https://clickhouse.com/docs/sql-reference/table-functions/s3",
        "https://clickhouse.com/docs/sql-reference/table-functions/s3Cluster",
        "https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem",
        "s3Cluster unavailable",
        "sink.auth.named_collection_missing",
        "format.parquet_read_unsupported",
        "source.columnar_writer_missing",
    ):
        assert expected in text


def test_route_capability_certification_is_linked_from_navigation_and_object_storage_docs() -> None:
    files = {
        "mkdocs": _read(ROOT / "mkdocs.yml"),
        "index": _read(DOCS / "index.md"),
        "object_storage": _read(DOCS / "object-storage-staging.md"),
        "cli_reference": _read(DOCS / "cli-reference.md"),
    }

    for name, text in files.items():
        assert "route-capability-certification" in text, name
    assert "Route capability certification" in files["mkdocs"]
