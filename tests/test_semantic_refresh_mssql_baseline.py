"""Create-only MSSQL persistence tests for complete baseline receipts."""

from __future__ import annotations

from dataclasses import fields
from typing import Any

import pytest

from dpone.adapters.semantic_refresh_mssql_baseline import (
    MssqlSemanticRefreshBaselineReceiptStore,
    SemanticRefreshMssqlBaselineReceiptError,
)
from dpone.contracts.semantic_refresh_baseline_receipt import (
    BaselineAssuranceKind,
    SemanticRefreshBaselineAdoptionReceipt,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _receipt() -> SemanticRefreshBaselineAdoptionReceipt:
    return SemanticRefreshBaselineAdoptionReceipt.build(
        model_unique_id="model.dp.events",
        baseline_kind=BaselineAssuranceKind.ADOPTED_COMPLETE_RELATION_CONFORMANT,
        release_id=_digest("1"),
        deployment_id=_digest("2"),
        source_snapshot_sha256=_digest("3"),
        source_relation_id="source.analytics.events",
        source_generation=3,
        mssql_relation_id="DWH.mart.events",
        mssql_generation=4,
        clickhouse_relation_id="analytics.mart.events",
        clickhouse_generation=8,
        clickhouse_target_uuid="0198f11c-6956-74f2-984b-4cfcb1653b87",
        mssql_connection_authority_id="mssql-prod",
        mssql_target_authority_id="mssql://mssql-prod/DWH/mart.events",
        clickhouse_cluster_authority_id="clickhouse-prod",
        clickhouse_target_authority_id="clickhouse://clickhouse-prod/analytics/mart.events",
        source_schema_sha256=_digest("4"),
        mssql_schema_sha256=_digest("5"),
        clickhouse_schema_sha256=_digest("6"),
        source_key_sha256=_digest("7"),
        mssql_key_sha256=_digest("8"),
        clickhouse_key_sha256=_digest("9"),
        source_physical_sha256=_digest("a"),
        mssql_physical_sha256=_digest("b"),
        clickhouse_physical_sha256=_digest("c"),
        coverage_start="2026-08-01T00:00:00Z",
        coverage_end="2026-08-08T00:00:00Z",
        source_coverage_sha256=_digest("d"),
        mssql_coverage_sha256=_digest("e"),
        clickhouse_coverage_sha256=_digest("f"),
        source_assurance_sha256=_digest("1"),
        mssql_assurance_sha256=_digest("2"),
        clickhouse_assurance_sha256=_digest("3"),
        utc_assurance_sha256=_digest("4"),
        writer_assurance_sha256=_digest("5"),
        ddl_assurance_sha256=_digest("6"),
        certified_codec_mapping_sha256=_digest("7"),
        historical_clickhouse_internal_multiset_evidence_sha256=_digest("8"),
        adopted_at="2026-08-08T00:00:00Z",
    )


class _Cursor:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self.row = row
        self.executions: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        self.executions.append((sql, parameters))
        if "OUTPUT inserted.baseline_receipt_sha256" in sql:
            self.row = (parameters[3],)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row

    def close(self) -> None:
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.autocommit = True
        self.cursor_instance = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def _row(receipt: SemanticRefreshBaselineAdoptionReceipt) -> tuple[object, ...]:
    import json

    return (
        receipt.model_unique_id,
        receipt.baseline_kind.value,
        receipt.baseline_adoption_receipt_sha256,
        json.dumps(receipt.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        "COMPLETE",
        True,
    )


def test_baseline_store_persists_and_replays_only_the_exact_complete_receipt() -> None:
    receipt = _receipt()
    first = _Connection(_Cursor())
    replay = _Connection(_Cursor(_row(receipt)))
    connections = iter((first, replay))
    store = MssqlSemanticRefreshBaselineReceiptStore(lambda: next(connections))

    assert store.persist_exact(receipt) == receipt
    assert store.persist_exact(receipt) == receipt
    assert first.commits == 1
    assert replay.commits == 1
    assert any(sql.startswith("INSERT INTO") for sql, _ in first.cursor_instance.executions)
    assert not any(sql.startswith("INSERT INTO") for sql, _ in replay.cursor_instance.executions)


def test_baseline_store_loads_and_revalidates_the_canonical_receipt_document() -> None:
    receipt = _receipt()
    connection = _Connection(_Cursor(_row(receipt)))
    store = MssqlSemanticRefreshBaselineReceiptStore(lambda: connection)

    loaded = store.load_exact(
        mssql_connection_authority_id="mssql-prod",
        mssql_relation_id="DWH.mart.events",
    )

    assert loaded == receipt
    assert connection.commits == 1


@pytest.mark.parametrize(
    "changed",
    [
        ("model.other",),
        ("model.dp.events", "adopted_complete_relation_conformant", _digest("9")),
    ],
)
def test_baseline_store_rejects_absent_or_conflicting_durable_state(changed: tuple[Any, ...]) -> None:
    receipt = _receipt()
    row = (*changed, *_row(receipt)[len(changed) :])
    connection = _Connection(_Cursor(row))
    store = MssqlSemanticRefreshBaselineReceiptStore(lambda: connection)

    with pytest.raises(SemanticRefreshMssqlBaselineReceiptError, match="differs"):
        store.persist_exact(receipt)

    assert connection.rollbacks == 1


def test_baseline_store_rejects_cross_wired_mssql_physical_identity_before_io() -> None:
    receipt = _receipt()
    connection = _Connection(_Cursor())
    store = MssqlSemanticRefreshBaselineReceiptStore(lambda: connection)

    with pytest.raises(ValueError, match="target authority"):
        store.persist_exact(
            SemanticRefreshBaselineAdoptionReceipt.build(
                **{
                    item.name: getattr(receipt, item.name)
                    for item in fields(receipt)
                    if item.name
                    not in {
                        "baseline_adoption_receipt_sha256",
                        "historical_clickhouse_internal_multiset_conformance",
                        "historical_cross_engine_payload_value_equivalence",
                        "mssql_target_authority_id",
                        "schema",
                        "status",
                    }
                },
                mssql_target_authority_id="mssql://other/DWH/mart.events",
            )
        )

    assert connection.cursor_instance.executions == []
