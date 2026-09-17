"""Recoverable target-local publication for generic ClickHouse full refresh."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Protocol

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.runtime.sinks import clickhouse_full_refresh_contract as publication_contract
from dpone.runtime.sinks.clickhouse_full_refresh_catalog import (
    ClickHouseFullRefreshCatalog,
    ClickHousePublicationTable,
)
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDesign
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult

REPLAY_OPTION = "__dpone_clickhouse_full_refresh_replay_v1"
_ADMITTED_DATABASE_ENGINES = frozenset({"Atomic", "Shared"})
_ZERO_UUID = "00000000-0000-0000-0000-000000000000"


class FullRefreshPublicationCatalog(Protocol):
    """Minimal catalog/DDL capability used by the publication algorithm."""

    def database_engine(self, database: str) -> str | None: ...

    def tables(self, database: str, names: tuple[str, ...]) -> dict[str, ClickHousePublicationTable]: ...

    def create_marker(self, database: str, marker: str, comment: str) -> None: ...

    def exchange(self, database: str, target: str, candidate: str) -> None: ...

    def rename(self, database: str, candidate: str, target: str) -> None: ...

    def drop(self, database: str, table: str) -> None: ...

    def count(self, database: str, table: str) -> int: ...


class ClickHouseFullRefreshOutcomeUnknown(publication_contract.ClickHouseFullRefreshPublicationError):
    """The catalog cannot prove whether the target mutation committed."""

    safe_to_retry = False
    operator_verification_required = True


@dataclass(frozen=True, slots=True)
class FullRefreshPublicationReceipt:
    """Verified committed mapping retained until exact cleanup succeeds."""

    marker: publication_contract.FullRefreshPublicationMarker
    marker_table: str
    recovered_after_error: bool = False

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> FullRefreshPublicationReceipt:
        expected = {"marker", "marker_table", "recovered_after_error"}
        if set(raw) != expected or not isinstance(raw.get("marker"), Mapping):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_RECEIPT_INVALID", "receipt fields do not match v1"
            )
        marker = publication_contract.FullRefreshPublicationMarker.from_json(_canonical_mapping_json(raw["marker"]))
        marker_table = raw.get("marker_table")
        recovered = raw.get("recovered_after_error")
        if not isinstance(marker_table, str) or not marker_table or not isinstance(recovered, bool):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_RECEIPT_INVALID", "receipt identity is invalid"
            )
        return cls(marker=marker, marker_table=marker_table, recovered_after_error=recovered)

    def to_dict(self) -> dict[str, Any]:
        return {
            "marker": self.marker.to_dict(),
            "marker_table": self.marker_table,
            "recovered_after_error": self.recovered_after_error,
        }


class ClickHouseFullRefreshPublicationService:
    """Publish once and decide every retry from exact ClickHouse catalog truth."""

    def __init__(self, catalog: FullRefreshPublicationCatalog) -> None:
        self._catalog = catalog

    @classmethod
    def from_connector(cls, connector: Any) -> ClickHouseFullRefreshPublicationService:
        """Compose the narrow catalog adapter at the ClickHouse boundary."""

        return cls(ClickHouseFullRefreshCatalog(connector))

    def prepare_admission(self, load_config: Any) -> Any:
        """Preflight or reconcile an existing operation before source I/O."""

        if not self.is_enabled(load_config):
            return load_config
        self._require_environment(load_config)
        database, target = str(load_config.target_schema), str(load_config.target_table)
        marker_table = publication_contract.publication_marker_name(target)
        marker_record = self._catalog.tables(database, (marker_table,)).get(marker_table)
        if marker_record is None:
            return load_config
        marker = self._owned_marker(load_config, marker_record)
        receipt = self._reconcile_or_publish(marker, marker_table=marker_table)
        result = self._replay_result(receipt)
        self.cleanup(receipt)
        options = dict(getattr(load_config, "options", {}) or {})
        options[REPLAY_OPTION] = result
        return replace(load_config, options=options)

    @staticmethod
    def replay_result(load_config: Any) -> LoadResult | None:
        result = (getattr(load_config, "options", {}) or {}).get(REPLAY_OPTION)
        if result is None:
            return None
        if not isinstance(result, LoadResult):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_RECEIPT_INVALID", "runtime replay option is invalid"
            )
        return result

    @staticmethod
    def is_enabled(load_config: Any) -> bool:
        """Admit only the bounded route whose compiler requires this protocol."""

        options = getattr(load_config, "options", {}) or {}
        return load_config.load_strategy is LoadStrategy.FULL_REFRESH and SOURCE_BYTE_BUDGET_OPTION in options

    def publish(self, load_config: Any, candidate_config: Any, *, staged_rows: int) -> FullRefreshPublicationReceipt:
        """Create immutable intent, execute one DDL, then prove the committed UUID mapping."""

        self._require_environment(load_config)
        database, target = str(load_config.target_schema), str(load_config.target_table)
        candidate_database = str(candidate_config.target_schema)
        candidate = str(candidate_config.target_table)
        if candidate_database != database:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_DATABASE_MISMATCH", "candidate must be target-local"
            )
        marker_table = publication_contract.publication_marker_name(target)
        observed = self._catalog.tables(database, (target, candidate, marker_table))
        desired = self._required_table(observed, candidate)
        predecessor = observed.get(target)
        self._require_supported_table(desired, role="candidate")
        if predecessor is not None:
            self._require_supported_table(predecessor, role="target")
        marker = publication_contract.FullRefreshPublicationMarker.create(
            operation_id=self._operation_id(load_config),
            database=database,
            target=target,
            candidate=candidate,
            predecessor_uuid=predecessor.uuid if predecessor is not None else None,
            desired_uuid=desired.uuid,
            staged_rows=staged_rows,
        )
        marker_record = observed.get(marker_table)
        if marker_record is None:
            try:
                self._catalog.create_marker(database, marker_table, marker.to_json())
            except Exception:
                marker_record = self._catalog.tables(database, (marker_table,)).get(marker_table)
                if marker_record is None:
                    raise
        marker_record = marker_record or self._required_table(
            self._catalog.tables(database, (marker_table,)), marker_table
        )
        persisted = self._parse_marker(marker_record)
        if persisted != marker:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OWNERSHIP_CONFLICT", "marker belongs to another publication plan"
            )
        return self._reconcile_or_publish(marker, marker_table=marker_table)

    def cleanup(self, receipt: FullRefreshPublicationReceipt | Mapping[str, Any]) -> None:
        """Drop only the exact predecessor and marker proven by a committed receipt."""

        resolved = (
            receipt
            if isinstance(receipt, FullRefreshPublicationReceipt)
            else FullRefreshPublicationReceipt.from_mapping(receipt)
        )
        marker = resolved.marker
        records = self._catalog.tables(marker.database, (marker.target, marker.candidate, resolved.marker_table))
        self._require_exact_marker(records.get(resolved.marker_table), marker)
        if self._state(marker, records) is not publication_contract.PublicationState.COMMITTED:
            raise ClickHouseFullRefreshOutcomeUnknown(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OUTCOME_UNKNOWN", "cleanup requires committed UUID mapping"
            )
        if marker.predecessor_uuid is not None:
            predecessor = self._required_table(records, marker.candidate)
            if predecessor.uuid != marker.predecessor_uuid:
                raise publication_contract.ClickHouseFullRefreshPublicationError(
                    "DPONE_CLICKHOUSE_FULL_REFRESH_CLEANUP_CONFLICT", "candidate UUID no longer owns predecessor"
                )
            self._drop_and_require_absent(marker.database, marker.candidate)
        self._require_exact_marker(
            self._catalog.tables(marker.database, (resolved.marker_table,)).get(resolved.marker_table), marker
        )
        self._drop_and_require_absent(marker.database, resolved.marker_table)

    def _reconcile_or_publish(
        self,
        marker: publication_contract.FullRefreshPublicationMarker,
        *,
        marker_table: str,
    ) -> FullRefreshPublicationReceipt:
        names = (marker.target, marker.candidate, marker_table)
        records = self._catalog.tables(marker.database, names)
        self._require_exact_marker(records.get(marker_table), marker)
        state = self._state(marker, records)
        if state is publication_contract.PublicationState.COMMITTED:
            return FullRefreshPublicationReceipt(marker=marker, marker_table=marker_table)
        if state is not publication_contract.PublicationState.PENDING:
            raise ClickHouseFullRefreshOutcomeUnknown(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OUTCOME_UNKNOWN", "catalog mapping is neither pending nor committed"
            )
        self._require_supported_table(self._required_table(records, marker.candidate), role="candidate")
        if marker.predecessor_uuid is not None:
            self._require_supported_table(self._required_table(records, marker.target), role="target")
        raised = False
        try:
            if marker.predecessor_uuid is None:
                self._catalog.rename(marker.database, marker.candidate, marker.target)
            else:
                self._catalog.exchange(marker.database, marker.target, marker.candidate)
        except Exception:
            raised = True
        observed = self._catalog.tables(marker.database, names)
        self._require_exact_marker(observed.get(marker_table), marker)
        if self._state(marker, observed) is not publication_contract.PublicationState.COMMITTED:
            raise ClickHouseFullRefreshOutcomeUnknown(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OUTCOME_UNKNOWN", "publication DDL completion is not proven"
            )
        return FullRefreshPublicationReceipt(
            marker=marker,
            marker_table=marker_table,
            recovered_after_error=raised,
        )

    def _replay_result(self, receipt: FullRefreshPublicationReceipt) -> LoadResult:
        marker = receipt.marker
        total = self._catalog.count(marker.database, marker.target)
        return LoadResult(
            inserted_rows=marker.staged_rows,
            updated_rows=0,
            total_rows=total,
            staging_rows=marker.staged_rows,
            commit_receipt_id=marker.operation_id,
            commit_outcome=AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
            reconciliation_metrics={"clickhouse_full_refresh_publication": receipt.to_dict()},
        )

    def _require_environment(self, load_config: Any) -> None:
        design = ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {})
        if design.cluster.on_cluster:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_TOPOLOGY_UNSUPPORTED", "cluster-wide publication is not certified"
            )
        database = str(load_config.target_schema)
        engine = self._catalog.database_engine(database)
        if engine not in _ADMITTED_DATABASE_ENGINES:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_ENGINE_UNSUPPORTED",
                f"database engine {engine or 'missing'} does not support admitted exchange",
            )

    def _owned_marker(
        self,
        load_config: Any,
        record: ClickHousePublicationTable,
    ) -> publication_contract.FullRefreshPublicationMarker:
        marker = self._parse_marker(record)
        expected = self._operation_id(load_config)
        if marker.operation_id != expected:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OWNERSHIP_CONFLICT", "another run owns unresolved publication"
            )
        if marker.database != str(load_config.target_schema) or marker.target != str(load_config.target_table):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OWNERSHIP_CONFLICT", "marker target identity mismatch"
            )
        return marker

    @staticmethod
    def _parse_marker(
        record: ClickHousePublicationTable,
    ) -> publication_contract.FullRefreshPublicationMarker:
        if record.engine != "TinyLog":
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_MARKER_INVALID", "marker table engine mismatch"
            )
        return publication_contract.FullRefreshPublicationMarker.from_json(record.comment)

    def _require_exact_marker(
        self,
        record: ClickHousePublicationTable | None,
        marker: publication_contract.FullRefreshPublicationMarker,
    ) -> None:
        if record is None or self._parse_marker(record) != marker:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_OWNERSHIP_CONFLICT", "publication marker changed"
            )

    @staticmethod
    def _state(
        marker: publication_contract.FullRefreshPublicationMarker,
        records: Mapping[str, ClickHousePublicationTable],
    ) -> publication_contract.PublicationState:
        target = records.get(marker.target)
        candidate = records.get(marker.candidate)
        return publication_contract.classify_publication(
            marker,
            target_uuid=target.uuid if target is not None else None,
            candidate_uuid=candidate.uuid if candidate is not None else None,
        )

    @staticmethod
    def _required_table(records: Mapping[str, ClickHousePublicationTable], table: str) -> ClickHousePublicationTable:
        record = records.get(table)
        if record is None or not record.uuid or record.uuid == _ZERO_UUID:
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_CATALOG_INVALID", f"table {table} has no stable UUID"
            )
        return record

    @staticmethod
    def _require_supported_table(record: ClickHousePublicationTable, *, role: str) -> None:
        if record.engine == "Distributed" or record.engine.startswith("Replicated"):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_TOPOLOGY_UNSUPPORTED",
                f"{role} engine {record.engine} requires separate topology certification",
            )

    def _drop_and_require_absent(self, database: str, table: str) -> None:
        try:
            self._catalog.drop(database, table)
        except Exception:
            if table in self._catalog.tables(database, (table,)):
                raise
            return
        if table in self._catalog.tables(database, (table,)):
            raise publication_contract.ClickHouseFullRefreshPublicationError(
                "DPONE_CLICKHOUSE_FULL_REFRESH_CLEANUP_PENDING", f"table {table} still exists after DROP"
            )

    @staticmethod
    def _operation_id(load_config: Any) -> str:
        options = getattr(load_config, "options", {}) or {}
        return publication_contract.publication_operation_id(
            run_id=str(options.get("run_id") or ""),
            database=str(load_config.target_schema),
            target=str(load_config.target_table),
        )


def _canonical_mapping_json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=True, sort_keys=True, separators=(",", ":"))


__all__ = [
    "REPLAY_OPTION",
    "ClickHouseFullRefreshOutcomeUnknown",
    "ClickHouseFullRefreshPublicationService",
    "FullRefreshPublicationCatalog",
    "FullRefreshPublicationReceipt",
]
