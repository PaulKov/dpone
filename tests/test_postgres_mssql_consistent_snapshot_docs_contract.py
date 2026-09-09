from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_current_postgres_mssql_guidance_requires_one_consistent_snapshot() -> None:
    performance = _read("docs/performance.md")
    mssql = _read("docs/mssql.md")
    route = _read("docs/source-sink/postgres-to-mssql.md")

    for document in (performance, mssql, route):
        assert "shared MVCC snapshot coordinator" in document

    assert "--num-partitions" not in performance
    assert "For very large PostgreSQL -> MSSQL and MSSQL -> ClickHouse transfers" not in mssql
    assert "COPY each partition TO STDOUT" not in route
    assert "Parallel PostgreSQL COPY workers" not in route
