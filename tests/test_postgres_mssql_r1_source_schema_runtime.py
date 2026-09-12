"""Fake-protocol runtime tests for selected-relation schema authority."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any

import pytest
from psycopg.pq import TransactionStatus

from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlTypePolicyAuthorityV1
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import PostgresMssqlSourceScalarShapeV1
from dpone.contracts.postgres_source_authority import PostgresSourceAuthority
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import bind_behavior_scenario


def _shape(
    oid: int = 20,
    *,
    family=PostgresMssqlSourceScalarFamilyV1.INT8,
    typmod: int = -1,
    precision: int | None = None,
    length_kind: PostgresMssqlLengthKindV1 = PostgresMssqlLengthKindV1.NOT_APPLICABLE,
) -> PostgresMssqlSourceScalarShapeV1:
    return PostgresMssqlSourceScalarShapeV1(
        family,
        oid,
        typmod,
        length_kind,
        precision,
        None,
        None,
    )


def _policy(shape: PostgresMssqlSourceScalarShapeV1 | None = None) -> PostgresMssqlTypePolicyAuthorityV1:
    selected = shape or _shape()
    return PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(selected, maximum_input_bytes=1024),),
        (selected,),
    )


def _source_authority(*, version: int = 1) -> PostgresSourceAuthority:
    common = {
        "version": version,
        "topology_role": "primary",
        "database": {"canonical_name": "warehouse", "oid": 16384},
        "principals": {
            "effective": {"canonical_name": "dpone_reader", "oid": 17001},
            "session": {"canonical_name": "dpone_login", "oid": 17002},
        },
        "relations": {
            "sales.orders": {"schema": "sales", "relation": "orders", "namespace_oid": 2200, "relation_oid": 22001}
        },
    }
    if version == 1:
        common.update(system_identifier="7272727272727272727", timeline_id=7)
    else:
        common["verification_profile"] = "catalog_identity"
    return PostgresSourceAuthority.from_connection_properties({"postgres_source_authority": common})


class FakeDriverError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__("secret vendor text")
        self.sqlstate = sqlstate


class FakeConnection:
    def __init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.autocommit = True
        self.closed = False
        self.close_attempts = 0
        self.close_error: BaseException | None = None

    def close(self) -> None:
        self.close_attempts += 1
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class FakeCatalogConnector:
    """Execute exact bound SQL while retaining protocol-visible events."""

    def __init__(self, *, columns: int = 1) -> None:
        self._connection: FakeConnection | None = FakeConnection()
        self.events: list[tuple[str, Any, tuple[object, ...]]] = []
        self.columns = columns
        self.attribute_numbers = tuple(range(1, columns + 1))
        self.backend_pid = 771
        self.fail_sqlstate: str | None = None
        self.rollback_error: BaseException | None = None
        self.quarantined = False
        self.copy_calls = 0
        self.incarnation = "9/41"
        self.snapshot_token = "10:20:"
        self.relation_present = True
        self.relkind = "r"
        self.relpersistence = "p"
        self.relhassubclass = False
        self.column_suffix = ""
        self.raise_next: BaseException | None = None

    @property
    def connection(self) -> FakeConnection:
        if self.quarantined or self._connection is None:
            raise RuntimeError("postgres_connector.quarantined")
        return self._connection

    def begin(self) -> None:
        self.events.append(("begin", "", ()))
        self.connection.autocommit = False
        self.connection.info.transaction_status = TransactionStatus.INTRANS

    def execute_query(self, query: Any, params: tuple[object, ...] = ()) -> int:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.events.append(("execute", text, tuple(params)))
        self._maybe_fail()
        return 0

    def get_records(self, query: Any, params: tuple[object, ...] = (), as_dict: bool = False) -> list[Any]:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.events.append(("records", text, tuple(params)))
        self._maybe_fail()
        rows = self._rows(text)
        assert as_dict is True
        return rows

    def rollback(self) -> None:
        self.events.append(("rollback", "", ()))
        if self.rollback_error is not None:
            raise self.rollback_error
        self.connection.info.transaction_status = TransactionStatus.IDLE
        self.connection.autocommit = True

    def quarantine(self) -> None:
        self.events.append(("quarantine", "", ()))
        physical = self._connection
        self._connection = None
        self.quarantined = True
        if physical is not None:
            try:
                physical.close()
            except Exception:
                pass

    def _maybe_fail(self) -> None:
        if self.raise_next is not None:
            pending = self.raise_next
            self.raise_next = None
            raise pending
        if self.fail_sqlstate is not None:
            sqlstate = self.fail_sqlstate
            self.fail_sqlstate = None
            raise FakeDriverError(sqlstate)

    def _rows(self, sql: str) -> list[dict[str, Any]]:
        if "snapshot_witness" in sql:
            return [
                {
                    "snapshot_token": self.snapshot_token,
                    "visible_horizon": 20,
                    "transaction_incarnation": self.incarnation,
                    "isolation_level": "repeatable read",
                    "read_only": "on",
                    "backend_pid": self.backend_pid,
                    "database_name": "warehouse",
                    "database_oid": 16384,
                    "effective_principal": "dpone_reader",
                    "effective_principal_oid": 17001,
                    "session_principal": "dpone_login",
                    "session_principal_oid": 17002,
                    "in_recovery": False,
                    "namespace_oid": 2200,
                    "schema_name": "sales",
                    "relation_oid": 22001,
                    "relation_name": "orders",
                    "lock_witness_count": 1,
                }
            ]
        if "pg_control_system" in sql:
            return [{"system_identifier": "7272727272727272727", "timeline_id": 7}]
        if "txid_current_snapshot" in sql and "lock_witness_count" in sql:
            return [
                {
                    "snapshot_token": self.snapshot_token,
                    "transaction_incarnation": self.incarnation,
                    "lock_witness_count": 1,
                }
            ]
        if "pg_attribute" in sql:
            return [
                self._column(attribute_number, suffix=self.column_suffix, name_index=projection_ordinal)
                for projection_ordinal, attribute_number in enumerate(self.attribute_numbers, start=1)
            ]
        if "c.relhassubclass" in sql:
            if not self.relation_present:
                return []
            return [
                {
                    "relation_oid": 22001,
                    "namespace_oid": 2200,
                    "schema_name": "sales",
                    "relation_name": "orders",
                    "relkind": self.relkind,
                    "relpersistence": self.relpersistence,
                    "relhassubclass": self.relhassubclass,
                }
            ]
        raise AssertionError(f"unexpected SQL: {sql}")

    @staticmethod
    def _column(index: int, *, suffix: str = "", name_index: int | None = None) -> dict[str, Any]:
        return {
            "relation_oid": 22001,
            "namespace_oid": 2200,
            "relkind": "r",
            "relpersistence": "p",
            "relhassubclass": False,
            "attribute_number": index,
            "column_name": f"column_{name_index or index}{suffix}",
            "type_oid": 20,
            "type_namespace_oid": 11,
            "type_namespace_name": "pg_catalog",
            "type_name": "int8",
            "type_kind": "b",
            "type_modifier": -1,
            "nullable": False,
            "collation_oid": 0,
            "generated_kind": "",
            "identity_kind": "",
        }


def _load_config() -> SimpleNamespace:
    return SimpleNamespace(source_schema="sales", source_table="orders", options={})


def issue_authority(
    modules: dict[str, Any],
    *,
    connector: FakeCatalogConnector | None = None,
    policy: PostgresMssqlTypePolicyAuthorityV1 | None = None,
):
    connector = connector or FakeCatalogConnector()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    schema_issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(
        type_policy_authority=policy or _policy()
    )
    profile = schema_issuer.bind_query_profile(selected_source_authority=selected)
    scope_issuer = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()
    scope = scope_issuer.open(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    authority = schema_issuer.issue(
        connector=connector,
        verified_relation=scope.require_active(connector),
        query_profile=profile,
    )
    return authority, scope, connector, schema_issuer, profile


def _all_true(coverage: tuple[str, ...]) -> dict[str, bool]:
    return {item: True for item in coverage}


def _runtime_lock(modules: dict[str, Any]) -> dict[str, bool]:
    _authority, scope, connector, _issuer, profile = issue_authority(modules)
    events = connector.events
    begin = next(i for i, event in enumerate(events) if event[0] == "begin")
    lock = next(i for i, event in enumerate(events) if "LOCK TABLE ONLY" in event[1])
    witness = next(i for i, event in enumerate(events) if "snapshot_witness" in event[1])
    lock_text = events[lock][1]
    assert begin < lock < witness
    assert profile.query_execution_profile_sha256
    scope.close_if_active()
    return {
        "begin": begin == 0,
        "repeatable_read_read_only": any("REPEATABLE READ READ ONLY" in event[1] for event in events),
        "lock_timeout": any("5000ms" in event[1] for event in events),
        "psycopg_identifier": '"sales"."orders"' in lock_text,
        "ONLY": "LOCK TABLE ONLY" in lock_text,
        "access_share": "ACCESS SHARE MODE" in lock_text,
        "first_snapshot_query_after_lock": lock < witness,
    }


def _runtime_revalidation(modules: dict[str, Any]) -> dict[str, bool]:
    _authority, scope, connector, _issuer, _profile = issue_authority(modules)
    before = len(connector.events)
    scope.require_active(connector)
    event = connector.events[-1]
    scope.close_if_active()
    return {
        "snapshot_token": "txid_current_snapshot" in event[1],
        "transaction_status": connector.events[before][0] == "records",
        "lock_witness": "AccessShareLock" in event[1],
        "relation_oid": event[2] == (22001,),
        "connector_identity": before < len(connector.events),
    }


def _runtime_catalog(modules: dict[str, Any]) -> dict[str, bool]:
    authority, scope, connector, _issuer, _profile = issue_authority(modules)
    row = connector._column(1)
    column = authority.ordered_columns[0]
    scope.close_if_active()
    return {
        "ordered_attnum": column.attribute_number == 1,
        "dropped_excluded": "NOT a.attisdropped" in next(e[1] for e in connector.events if "pg_attribute" in e[1]),
        "system_columns_excluded": "a.attnum > 0" in next(e[1] for e in connector.events if "pg_attribute" in e[1]),
        "type_namespace_join": column.type_namespace_oid == row["type_namespace_oid"],
        "same_scope": column.relation_oid == 22001,
    }


def _permission(modules: dict[str, Any], state: str, expected: str) -> dict[str, bool]:
    connector = FakeCatalogConnector()
    connector.fail_sqlstate = state
    try:
        issue_authority(modules, connector=connector)
    except Exception as exc:
        closed_translation = exc.__cause__ is None and exc.__context__ is None
        return {
            f"sqlstate.{state}": getattr(exc, "reason", "") == expected,
            expected: getattr(exc, "reason", "") == expected and closed_translation,
            "redacted_error" if state == "42501" else "retryable_source": (
                "secret vendor text" not in str(exc) and closed_translation
            ),
            **({"not_caller_cancelled": type(exc).__name__ != "CancelledError"} if state == "57014" else {}),
        }
    raise AssertionError("typed translation required")


def _same_scope(modules: dict[str, Any]) -> dict[str, bool]:
    first, scope, connector, issuer, profile = issue_authority(modules)
    second = issuer.issue(connector=connector, verified_relation=scope.require_active(connector), query_profile=profile)
    scope.close_if_active()
    return {
        "same_scope_repeat_issue": first is not second,
        "authority_canonical_bytes_equal": first.canonical_bytes == second.canonical_bytes,
        "authority_digest_equal": first.digest == second.digest,
    }


def _new_scope(modules: dict[str, Any]) -> dict[str, bool]:
    first, scope, connector, _issuer, _profile = issue_authority(modules)
    old_incarnation = connector.incarnation
    scope.close_if_active()
    connector.incarnation = "9/42"
    second, second_scope, *_ = issue_authority(modules, connector=connector)
    second_scope.close_if_active()
    return {
        "new_transaction_incarnation": old_incarnation != connector.incarnation,
        "same_catalog_authority_bytes": first.canonical_bytes == second.canonical_bytes,
        "same_source_digest": first.selected_source_authority_sha256 == second.selected_source_authority_sha256,
    }


def _single_active(modules: dict[str, Any]) -> dict[str, bool]:
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = issuer.bind_query_profile(selected_source_authority=selected)
    scope_issuer = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()
    connector = FakeCatalogConnector()
    entered = threading.Event()
    release = threading.Event()
    original_begin = connector.begin

    def blocking_begin() -> None:
        entered.set()
        assert release.wait(5)
        original_begin()

    connector.begin = blocking_begin  # type: ignore[method-assign]
    outcomes: list[object] = []

    def first() -> None:
        try:
            outcomes.append(
                scope_issuer.open(
                    connector=connector,
                    lifecycle=ExtractionLifecycleAuthority(),
                    selected_source_authority=selected,
                    query_profile=profile.generic_relation_profile,
                )
            )
        except BaseException as exc:
            outcomes.append(exc)

    thread = threading.Thread(target=first)
    thread.start()
    assert entered.wait(5)
    second = None
    try:
        scope_issuer.open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    except Exception as exc:
        second = exc
    release.set()
    thread.join()
    scope: Any = next(item for item in outcomes if not isinstance(item, BaseException))
    scope.close_if_active()
    return {
        "IDLE": connector.connection.info.transaction_status is TransactionStatus.IDLE,
        "OPENING": entered.is_set(),
        "ACTIVE": scope.terminal_receipt is not None,
        "typed_second_rejection": getattr(second, "reason", "") == "snapshot_lease_mismatch",
        "one_begin": sum(event[0] == "begin" for event in connector.events) == 1,
        "identity_checked_release": sum(event[0] == "rollback" for event in connector.events) == 1,
        "initially_busy_physical_session_rejected": _busy_rejects(scope_issuer, connector, selected, profile),
    }


def _busy_rejects(scope_issuer, connector, selected, profile) -> bool:
    connector.connection.info.transaction_status = TransactionStatus.INTRANS
    before = len(connector.events)
    try:
        scope_issuer.open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    except Exception as exc:
        return getattr(exc, "reason", "") == "snapshot_lease_mismatch" and len(connector.events) == before
    return False


def _session_incarnation(modules: dict[str, Any]) -> dict[str, bool]:
    _authority, scope, connector, *_ = issue_authority(modules)
    verified = scope.require_active(connector)
    first_incarnation = connector.incarnation
    connector.incarnation = "9/99"
    replaced = None
    try:
        scope.require_active(connector)
    except Exception as exc:
        replaced = exc
    connector.incarnation = first_incarnation
    connector.connection.info.transaction_status = TransactionStatus.IDLE
    committed = None
    try:
        scope.require_active(connector)
    except Exception as exc:
        committed = exc
    connector.connection.info.transaction_status = TransactionStatus.INTRANS
    connector.snapshot_token = "11:21:"
    rolled = None
    try:
        scope.require_active(connector)
    except Exception as exc:
        rolled = exc
    scope.close_if_active()
    return {
        "backend_pid": getattr(verified, "backend_pid", None) == connector.backend_pid,
        "virtualtransaction": getattr(verified, "transaction_incarnation", None) == first_incarnation,
        "replacement_transaction_rejected": getattr(replaced, "reason", "") == "snapshot_lease_mismatch",
        "out_of_band_commit_reuse_rejected": getattr(committed, "reason", "") == "snapshot_lease_mismatch",
        "out_of_band_rollback_reuse_rejected": getattr(rolled, "reason", "") == "snapshot_lease_mismatch",
    }


def _relation_profile(modules: dict[str, Any]) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for field, bad in (("relkind", "v"), ("relpersistence", "u"), ("relhassubclass", True)):
        connector = FakeCatalogConnector()
        setattr(connector, field, bad)
        try:
            issue_authority(modules, connector=connector)
        except Exception as exc:
            results[
                {
                    "relkind": "relkind.r",
                    "relpersistence": "relpersistence.p",
                    "relhassubclass": "relhassubclass.false",
                }[field]
            ] = getattr(exc, "reason", "") == "relation_profile_unsupported"
    connector = FakeCatalogConnector()
    connector.relation_present = False
    try:
        issue_authority(modules, connector=connector)
    except Exception as exc:
        results["name_oid_match"] = getattr(exc, "reason", "") == "source_authority_mismatch"
    return results


def _caller_cancel(modules: dict[str, Any]) -> dict[str, bool]:
    import asyncio

    connector = FakeCatalogConnector()
    cancellation = asyncio.CancelledError("caller")
    connector.raise_next = cancellation
    connector.rollback_error = KeyboardInterrupt("cleanup cancellation")
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    schema_issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = schema_issuer.bind_query_profile(selected_source_authority=selected)
    scope_issuer = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()
    caught = None
    try:
        scope_issuer.open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    except BaseException as exc:
        caught = exc
    reopened = scope_issuer.open(
        connector=FakeCatalogConnector(),
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    reopened.close_if_active()
    return {
        "caller_cancelled_unchanged": caught is cancellation,
        "cleanup_before_propagation": any(event[0] == "rollback" for event in connector.events)
        and reopened.terminal_receipt is not None,
        "no_authority_returned": caught is not None,
    }


def _cleanup_idempotence(modules: dict[str, Any]) -> dict[str, bool]:
    connector = FakeCatalogConnector()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    schema_issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = schema_issuer.bind_query_profile(selected_source_authority=selected)
    scope_issuer = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()

    first_scope = scope_issuer.open(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    first_scope.complete_after_artifact_seal()
    first_receipt = first_scope.terminal_receipt
    first_scope.complete_after_artifact_seal()

    connector.incarnation = "9/42"
    second_scope = scope_issuer.open(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    second_scope.abort_preserving(RuntimeError("primary"))
    second_receipt = second_scope.terminal_receipt
    second_scope.abort_preserving(RuntimeError("ignored repeat"))

    connector.incarnation = "9/43"
    third_scope = scope_issuer.open(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    third_scope.close_if_active()
    return {
        "complete_once": first_scope.terminal_receipt is first_receipt,
        "abort_once": second_scope.terminal_receipt is second_receipt,
        "close_once": sum(event[0] == "rollback" for event in connector.events) == 3,
        "same_terminal_receipt": first_receipt is not second_receipt
        and first_scope.terminal_receipt is first_receipt
        and second_scope.terminal_receipt is second_receipt,
        "reservation_release_once": third_scope.terminal_receipt is not None,
    }


def _quarantine(modules: dict[str, Any]) -> dict[str, bool]:
    _authority, scope, connector, *_ = issue_authority(modules)
    physical = connector.connection
    connector.rollback_error = RuntimeError("secret rollback")
    physical.close_error = RuntimeError("secret close")
    error = None
    try:
        scope.close_if_active()
    except Exception as exc:
        error = exc
    receipt = scope.terminal_receipt
    reuse = None
    try:
        _ = connector.connection
    except Exception as exc:
        reuse = exc
    return {
        "rollback_failure": error is not None,
        "detach_before_close": connector._connection is None,
        "quarantine_after_rollback_failure": connector.quarantined,
        "close_failure": physical.close_attempts == 1 and physical.closed is False,
        "permanent_quarantined_state": reuse is not None,
        "no_reconnect": connector._connection is None,
        "redacted_receipt": receipt.cleanup_succeeded is False and "secret" not in repr(receipt),
    }


def _ended_wrong(modules: dict[str, Any]) -> dict[str, bool]:
    _authority, completed, connector, *_ = issue_authority(modules)
    completed.close_if_active()
    before = len(connector.events)
    completed_error = None
    try:
        completed.require_active(connector)
    except Exception as exc:
        completed_error = exc
    _authority2, aborted, connector2, *_ = issue_authority(modules)
    aborted.abort_preserving(RuntimeError("primary"))
    aborted_error = None
    try:
        aborted.require_active(connector2)
    except Exception as exc:
        aborted_error = exc
    _authority3, active, connector3, *_ = issue_authority(modules)
    wrong_error = None
    try:
        active.require_active(FakeCatalogConnector())
    except Exception as exc:
        wrong_error = exc
    active.close_if_active()
    return {
        "completed_scope_rejected": getattr(completed_error, "reason", "") == "snapshot_lease_mismatch",
        "aborted_scope_rejected": getattr(aborted_error, "reason", "") == "snapshot_lease_mismatch",
        "wrong_connector_rejected": getattr(wrong_error, "reason", "") == "snapshot_lease_mismatch",
        "no_catalog_read": len(connector.events) == before,
    }


def _post_ddl(modules: dict[str, Any]) -> dict[str, bool]:
    first, first_scope, first_connector, *_ = issue_authority(modules)
    first_scope.close_if_active()
    second_connector = FakeCatalogConnector()
    second_connector.column_suffix = "_new"
    second, second_scope, *_ = issue_authority(modules, connector=second_connector)
    second_scope.close_if_active()
    return {
        "new_scope_after_ddl": first_connector is not second_connector,
        "changed_column_authority": first.ordered_columns[0].canonical_bytes
        != second.ordered_columns[0].canonical_bytes,
        "changed_aggregate_digest": first.digest != second.digest,
        "old_scope_not_reused": first_scope.terminal_receipt is not None,
    }


def _transient_retry(modules: dict[str, Any]) -> dict[str, bool]:
    first_connector = FakeCatalogConnector()
    first_connector.fail_sqlstate = "40001"
    first_error = None
    try:
        issue_authority(modules, connector=first_connector)
    except Exception as exc:
        first_error = exc
    second, scope, second_connector, *_ = issue_authority(modules)
    scope.close_if_active()
    return {
        "issuer_opening_failure_cleanup": first_error is not None,
        "catalog_extraction_failure_cleanup": any(e[0] == "rollback" for e in first_connector.events),
        "rollback_and_reservation_release": first_connector.connection.info.transaction_status
        is TransactionStatus.IDLE,
        "fresh_scope_retry": second_connector is not first_connector,
        "single_success_authority": len(second.ordered_columns) == 1,
    }


bind_behavior_scenario("runtime.lock-before-snapshot", _runtime_lock)
bind_behavior_scenario("runtime.single-active-reservation", _single_active)
bind_behavior_scenario("runtime.session-incarnation", _session_incarnation)
bind_behavior_scenario("runtime.revalidation", _runtime_revalidation)
bind_behavior_scenario("runtime.relation-profile", _relation_profile)
bind_behavior_scenario("runtime.catalog-observation", _runtime_catalog)
bind_behavior_scenario(
    "runtime.permission-translation", lambda modules: _permission(modules, "42501", "metadata_permission_denied")
)
bind_behavior_scenario("runtime.caller-cancellation", _caller_cancel)
bind_behavior_scenario(
    "runtime.database-cancellation-57014", lambda modules: _permission(modules, "57014", "catalog_observation_failed")
)
bind_behavior_scenario("runtime.cleanup-idempotence", _cleanup_idempotence)
bind_behavior_scenario("runtime.physical-quarantine", _quarantine)
bind_behavior_scenario("runtime.same-scope-byte-identity", _same_scope)
bind_behavior_scenario("runtime.ended-or-wrong-scope", _ended_wrong)
bind_behavior_scenario("runtime.new-scope-unchanged-identity", _new_scope)
bind_behavior_scenario("runtime.post-ddl-digest-change", _post_ddl)
bind_behavior_scenario("runtime.transient-observation-retry", _transient_retry)


@pytest.mark.parametrize("version", (1, 2))
def test_fake_protocol_uses_true_selected_source_versions(version: int) -> None:
    verifier = PostgresSourceAuthorityVerifier(_source_authority(version=version))
    if not hasattr(verifier, "select_for"):
        pytest.fail("approved read-only selected-source API is missing", pytrace=False)
    selected = verifier.select_for(_load_config())
    assert selected.version == version
    assert (selected.system_identifier is None) is (version == 2)


def test_fake_protocol_distinguishes_driver_cancellation_from_caller_cancellation() -> None:
    exception_group_type = getattr(__import__("builtins"), "BaseExceptionGroup")
    assert not isinstance(FakeDriverError("57014"), exception_group_type)


def test_concurrent_open_fixture_has_deterministic_barrier() -> None:
    barrier = threading.Barrier(2)
    seen: list[int] = []

    def worker(index: int) -> None:
        barrier.wait()
        seen.append(index)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(seen) == [0, 1]


def test_empty_transaction_incarnation_rejects_and_releases_issuer() -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    schema_issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = schema_issuer.bind_query_profile(selected_source_authority=selected)
    scope_issuer = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()
    invalid = FakeCatalogConnector()
    invalid.incarnation = ""
    error = None
    try:
        scope_issuer.open(
            connector=invalid,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    except Exception as exc:
        error = exc
    assert getattr(error, "reason", "") == "relation_lock_not_proven"
    assert error is not None
    assert error.__cause__ is error.__context__ is None
    assert sum(event[0] == "rollback" for event in invalid.events) == 1

    valid = FakeCatalogConnector()
    scope = scope_issuer.open(
        connector=valid,
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    scope.close_if_active()
    assert scope.terminal_receipt is not None


@pytest.mark.parametrize("rollback_fails", (False, True))
def test_partial_begin_idle_failure_restores_or_quarantines_and_releases_issuer(
    rollback_fails: bool,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    schema_issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = schema_issuer.bind_query_profile(selected_source_authority=selected)
    scope_issuer = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()

    class PartialBeginConnector(FakeCatalogConnector):
        def begin(self) -> None:
            self.events.append(("begin", "", ()))
            self.connection.autocommit = False
            self.connection.info.transaction_status = TransactionStatus.IDLE
            if rollback_fails:
                self.rollback_error = RuntimeError("secret rollback")
            raise RuntimeError("secret begin")

    connector = PartialBeginConnector()
    with pytest.raises(Exception) as raised:
        scope_issuer.open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )

    assert getattr(raised.value, "reason", "") == "internal_invariant_violation"
    assert raised.value.__cause__ is raised.value.__context__ is None
    assert sum(event[0] == "rollback" for event in connector.events) == 1
    if rollback_fails:
        assert connector.quarantined is True
        assert connector._connection is None
    else:
        assert connector.quarantined is False
        assert connector.connection.autocommit is True
        assert connector.connection.info.transaction_status is TransactionStatus.IDLE

    reopened = scope_issuer.open(
        connector=FakeCatalogConnector(),
        lifecycle=ExtractionLifecycleAuthority(),
        selected_source_authority=selected,
        query_profile=profile.generic_relation_profile,
    )
    reopened.close_if_active()
    assert reopened.terminal_receipt is not None


def test_real_postgres_connector_quarantine_detaches_before_failing_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    import psycopg

    from dpone.runtime.connectors.postgres import PostgresConnector

    connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")
    observations: list[tuple[bool, bool]] = []
    checks: list[tuple[str, bool]] = []

    class FailingPhysicalConnection:
        def close(self) -> None:
            observations.append(
                (
                    connector._connection is None,
                    getattr(connector, "_quarantined", False),
                )
            )
            raise RuntimeError("secret physical close")

    physical = FailingPhysicalConnection()
    connector._connection = physical
    monkeypatch.setattr(psycopg, "connect", lambda **_kwargs: pytest.fail("quarantined connector reconnected"))
    quarantine = getattr(connector, "quarantine", None)
    ordinary_caught = None
    try:
        if callable(quarantine):
            quarantine()
    except BaseException as error:
        ordinary_caught = error
    reuse_error = None
    try:
        _ = connector.connection
    except BaseException as error:
        reuse_error = error
    checks.extend(
        (
            ("public ordinary API exists", callable(quarantine)),
            ("public ordinary close failure contained", ordinary_caught is None),
            ("public ordinary detached before close", observations == [(True, True)]),
            ("public ordinary physical detached", connector._connection is None),
            (
                "public ordinary reuse blocked",
                isinstance(reuse_error, RuntimeError) and str(reuse_error) == "postgres_connector.quarantined",
            ),
        )
    )

    public_cancellation = asyncio.CancelledError("public quarantine close cancelled")
    public_cancelled_connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")

    class PublicCancelledPhysical:
        def __init__(self) -> None:
            self.close_attempts = 0

        def close(self) -> None:
            self.close_attempts += 1
            raise public_cancellation

    public_cancelled = PublicCancelledPhysical()
    public_cancelled_connector._connection = public_cancelled
    public_quarantine = getattr(public_cancelled_connector, "quarantine", None)
    public_caught = None
    try:
        if callable(public_quarantine):
            public_quarantine()
    except BaseException as error:
        public_caught = error
    checks.extend(
        (
            ("public cancellation API exists", callable(public_quarantine)),
            ("public cancellation identity", public_caught is public_cancellation),
            (
                "public cancellation links closed",
                public_caught is not None and public_caught.__cause__ is public_caught.__context__ is None,
            ),
            ("public cancellation detached", public_cancelled_connector._connection is None),
            (
                "public cancellation quarantined",
                getattr(public_cancelled_connector, "_quarantined", False) is True,
            ),
            ("public cancellation close once", public_cancelled.close_attempts == 1),
        )
    )

    exact_connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")

    class ExactPhysicalConnection:
        def __init__(self, close_error: BaseException | None = None) -> None:
            self.closed = False
            self.close_attempts = 0
            self.close_error = close_error

        def close(self) -> None:
            self.close_attempts += 1
            self.closed = True
            if self.close_error is not None:
                raise self.close_error

    exact = ExactPhysicalConnection()
    exact_connector._connection = exact
    exact_connector._quarantined = True
    quarantine_if_current = getattr(exact_connector, "quarantine_if_current", None)
    exact_result = quarantine_if_current(exact) if callable(quarantine_if_current) else None
    checks.extend(
        (
            ("exact API exists", callable(quarantine_if_current)),
            ("already quarantined exact current handled", exact_result is True),
            ("already quarantined exact current detached", exact_connector._connection is None),
            ("already quarantined state retained", getattr(exact_connector, "_quarantined", False) is True),
            ("already quarantined exact current closed once", (exact.close_attempts, exact.closed) == (1, True)),
        )
    )

    failing_exact_connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")
    failing_exact = ExactPhysicalConnection(RuntimeError("exact close secret"))
    failing_exact_connector._connection = failing_exact
    failing_exact_quarantine = getattr(failing_exact_connector, "quarantine_if_current", None)
    failing_exact_caught = None
    try:
        failing_exact_result = failing_exact_quarantine(failing_exact) if callable(failing_exact_quarantine) else None
    except BaseException as error:
        failing_exact_result = None
        failing_exact_caught = error
    checks.extend(
        (
            ("exact ordinary API exists", callable(failing_exact_quarantine)),
            ("exact ordinary close failure contained", failing_exact_caught is None),
            ("exact ordinary current handled", failing_exact_result is True),
            ("exact ordinary detached", failing_exact_connector._connection is None),
            (
                "exact ordinary quarantined",
                getattr(failing_exact_connector, "_quarantined", False) is True,
            ),
            ("exact ordinary close once", (failing_exact.close_attempts, failing_exact.closed) == (1, True)),
        )
    )

    replacement_connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")
    stale = ExactPhysicalConnection()
    replacement = ExactPhysicalConnection()
    replacement_connector._connection = replacement
    replacement_quarantine = getattr(replacement_connector, "quarantine_if_current", None)
    replacement_result = replacement_quarantine(stale) if callable(replacement_quarantine) else None
    checks.extend(
        (
            ("replacement API exists", callable(replacement_quarantine)),
            ("stale physical rejected", replacement_result is False),
            ("replacement retained", replacement_connector._connection is replacement),
            (
                "replacement connector remains reusable",
                getattr(replacement_connector, "_quarantined", False) is False,
            ),
            ("stale and replacement not closed", (stale.close_attempts, replacement.close_attempts) == (0, 0)),
        )
    )

    cancellation = asyncio.CancelledError("physical close cancelled")
    cancelled_connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")
    cancelled = ExactPhysicalConnection(cancellation)
    cancelled_connector._connection = cancelled
    cancelled_quarantine = getattr(cancelled_connector, "quarantine_if_current", None)
    caught = None
    try:
        if callable(cancelled_quarantine):
            cancelled_quarantine(cancelled)
    except BaseException as error:
        caught = error
    checks.extend(
        (
            ("exact cancellation API exists", callable(cancelled_quarantine)),
            ("exact cancellation identity", caught is cancellation),
            (
                "exact cancellation links closed",
                caught is not None and caught.__cause__ is caught.__context__ is None,
            ),
            ("exact cancellation detached", cancelled_connector._connection is None),
            (
                "exact cancellation quarantined",
                getattr(cancelled_connector, "_quarantined", False) is True,
            ),
            ("exact cancellation close once", (cancelled.close_attempts, cancelled.closed) == (1, True)),
        )
    )

    assert [name for name, passed in checks if not passed] == []


def test_real_postgres_connector_acquire_quarantine_race_never_returns_or_caches_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    from dpone.runtime.connectors.postgres import PostgresConnector

    connector = PostgresConnector("unused", 5432, "unused", "unused", "unused")
    connect_entered = threading.Event()
    release_connect = threading.Event()
    outcomes: list[object] = []

    class PhysicalConnection:
        def __init__(self) -> None:
            self.close_attempts = 0

        def close(self) -> None:
            self.close_attempts += 1

    physical = PhysicalConnection()

    def connect(*_args: object, **_kwargs: object) -> object:
        connect_entered.set()
        assert release_connect.wait(5)
        return physical

    monkeypatch.setattr(psycopg, "connect", connect)

    def acquire() -> None:
        try:
            outcomes.append(connector.connection)
        except BaseException as exc:
            outcomes.append(exc)

    thread = threading.Thread(target=acquire)
    thread.start()
    assert connect_entered.wait(5)
    quarantine = getattr(connector, "quarantine", None)
    assert callable(quarantine)
    quarantine()
    release_connect.set()
    thread.join(5)

    assert thread.is_alive() is False
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], RuntimeError)
    assert str(outcomes[0]) == "postgres_connector.quarantined"
    assert connector._connection is None
    assert connector._quarantined is True
    assert physical.close_attempts == 1


def test_contract_model_and_runtime_translations_drop_raw_exception_links() -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import (
        _SELECTED_AUTHORITY_GOLDENS,
        _feature_modules,
    )

    modules = _feature_modules()

    def caught(call) -> BaseException:
        try:
            call()
        except Exception as exc:
            assert type(exc) is not AssertionError
            return exc
        raise AssertionError("stable translated exception required")

    malformed_document = __import__("json").loads(_SELECTED_AUTHORITY_GOLDENS[1])
    malformed_document["database"]["oid"] = True
    failures = [
        caught(lambda: modules["models"].require_type_policy(object())),
        caught(lambda: modules["models"].PostgresSelectedRelationAuthorityDocumentV1.from_document(malformed_document)),
        caught(lambda: modules["authority"].PostgresMssqlSelectedRelationSchemaAuthorityV1.from_canonical_bytes(b"x")),
    ]
    for sqlstate in ("42501", "40001", "57014"):
        connector = FakeCatalogConnector()
        connector.fail_sqlstate = sqlstate
        failures.append(caught(lambda connector=connector: issue_authority(modules, connector=connector)))
    assert all(error.__cause__ is None and error.__context__ is None for error in failures)


def test_catalog_issuer_propagates_caller_cancellation_unchanged_after_cleanup() -> None:
    import asyncio

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = FakeCatalogConnector()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = issuer.bind_query_profile(selected_source_authority=selected)
    scope = (
        modules["snapshot"]
        .PostgresVerifiedRelationSnapshotIssuerV1()
        .open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    )
    verified = scope.require_active(connector)
    cancellation = asyncio.CancelledError("caller cancelled during catalog issuance")
    connector.raise_next = cancellation
    caught = None
    try:
        issuer.issue(
            connector=connector,
            verified_relation=verified,
            query_profile=profile,
        )
    except BaseException as error:
        caught = error
    finally:
        close_result = scope.close_if_active()

    receipt = scope.terminal_receipt
    assert caught is cancellation
    assert receipt is not None
    assert close_result is receipt
    assert (
        caught.__cause__,
        caught.__context__,
        receipt.outcome,
        receipt.cleanup_attempted,
        receipt.cleanup_succeeded,
        receipt.cleanup_error_reason,
        receipt.connection_quarantined,
        connector.quarantined,
        sum(event[0] == "rollback" for event in connector.events),
    ) == (None, None, "aborted", True, True, None, False, False, 1)


def test_projection_propagates_caller_cancellation_unchanged() -> None:
    import asyncio

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    authority, scope, connector, *_ = issue_authority(modules)
    cancellation = asyncio.CancelledError("caller cancelled during projection")
    factory_calls = 0

    def cancel_projection(**_kwargs: object) -> object:
        nonlocal factory_calls
        factory_calls += 1
        raise cancellation

    adapter = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(
        fetched_schema_factory=cancel_projection
    )
    events_before = tuple(connector.events)
    caught = None
    try:
        adapter.project(authority)
    except BaseException as error:
        caught = error
    finally:
        close_result = scope.close_if_active()

    receipt = scope.terminal_receipt
    non_cleanup_events = tuple(event for event in connector.events if event[0] not in {"rollback", "quarantine"})
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    assert caught is cancellation
    assert receipt is not None
    assert close_result is receipt
    assert rollback_count == 1
    assert connector.quarantined is False
    assert (
        caught.__cause__,
        caught.__context__,
        factory_calls,
        non_cleanup_events,
        receipt.outcome,
        receipt.cleanup_attempted,
        receipt.cleanup_succeeded,
        receipt.cleanup_error_reason,
        receipt.connection_quarantined,
        scope.close_if_active() is receipt,
        sum(event[0] == "rollback" for event in connector.events),
    ) == (None, None, 1, events_before, "aborted", True, True, None, False, True, rollback_count)


def test_internal_catalog_failure_is_route_owned_and_has_no_exception_links() -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = FakeCatalogConnector()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = issuer.bind_query_profile(selected_source_authority=selected)
    scope = (
        modules["snapshot"]
        .PostgresVerifiedRelationSnapshotIssuerV1()
        .open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    )
    verified = scope.require_active(connector)
    connector.raise_next = ValueError("secret internal catalog failure")
    with pytest.raises(Exception) as raised:
        issuer.issue(
            connector=connector,
            verified_relation=verified,
            query_profile=profile,
        )
    scope.close_if_active()

    assert type(raised.value) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    assert getattr(raised.value, "reason", "") == "internal_invariant_violation"
    assert raised.value.__cause__ is raised.value.__context__ is None


def test_internal_projection_failure_is_route_owned_and_has_no_exception_links() -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    authority, scope, *_ = issue_authority(modules)

    def fail_projection(**_kwargs: object) -> object:
        raise ValueError("secret internal projection failure")

    adapter = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(fetched_schema_factory=fail_projection)
    with pytest.raises(Exception) as raised:
        adapter.project(authority)
    scope.close_if_active()

    assert type(raised.value) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    assert getattr(raised.value, "reason", "") == "internal_invariant_violation"
    assert raised.value.__cause__ is raised.value.__context__ is None


@pytest.mark.parametrize(
    ("row_changes", "expected_reason"),
    (
        ({"type_namespace_oid": 999, "type_namespace_name": "attacker"}, "column_type_identity_invalid"),
        ({"type_oid": 23, "type_name": "int4"}, "type_policy_mismatch"),
        ({"type_kind": "d"}, "source_column_unsupported"),
    ),
)
def test_catalog_failure_reason_precedence_is_exact(row_changes: dict[str, object], expected_reason: str) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = FakeCatalogConnector()
    original = connector._column

    def changed_column(index: int, *, suffix: str = "", name_index: int | None = None) -> dict[str, Any]:
        row = original(index, suffix=suffix, name_index=name_index)
        row.update(row_changes)
        return row

    connector._column = changed_column  # type: ignore[method-assign]
    with pytest.raises(Exception) as raised:
        issue_authority(modules, connector=connector)

    assert type(raised.value) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    assert getattr(raised.value, "reason", "") == expected_reason
    assert raised.value.__cause__ is raised.value.__context__ is None


@pytest.mark.parametrize(
    ("shape", "type_name", "target_collation", "datetime_precision"),
    (
        (
            _shape(
                25,
                family=PostgresMssqlSourceScalarFamilyV1.TEXT,
                length_kind=PostgresMssqlLengthKindV1.MAXIMUM,
            ),
            "text",
            "Latin1_General_100_BIN2",
            None,
        ),
        (
            _shape(1114, family=PostgresMssqlSourceScalarFamilyV1.TIMESTAMP, typmod=3, precision=3),
            "timestamp",
            None,
            3,
        ),
    ),
)
def test_projection_preserves_governed_collation_and_datetime_precision(
    shape: PostgresMssqlSourceScalarShapeV1,
    type_name: str,
    target_collation: str | None,
    datetime_precision: int | None,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = FakeCatalogConnector()
    original = connector._column

    def shaped_column(index: int, *, suffix: str = "", name_index: int | None = None) -> dict[str, Any]:
        row = original(index, suffix=suffix, name_index=name_index)
        row.update(
            type_oid=shape.source_type_oid,
            type_name=type_name,
            type_modifier=shape.source_typmod,
            collation_oid=100 if target_collation is not None else 0,
        )
        return row

    connector._column = shaped_column  # type: ignore[method-assign]
    authority, scope, *_ = issue_authority(modules, connector=connector, policy=_policy(shape))
    adapter = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(
        fetched_schema_factory=modules["boundary"].build_r1_postgres_fetched_schema
    )
    projection = adapter.project(authority)
    scope.close_if_active()

    assert projection.target_projection.columns[0].collation == target_collation
    assert projection.relation_metadata[0].datetime_precision == datetime_precision


@pytest.mark.parametrize("terminal", ("completed", "aborted"))
def test_copy_admission_rejects_completed_or_aborted_lifecycle(terminal: str) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    lifecycle = ExtractionLifecycleAuthority()
    connector = FakeCatalogConnector()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    runtime = modules["runtime"].PostgresMssqlSourceSchemaRuntimeV1(
        verifier=verifier,
        snapshot_scope_issuer=modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1(),
        schema_authority_issuer=modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(
            type_policy_authority=_policy()
        ),
        projection_adapter=modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(
            fetched_schema_factory=modules["boundary"].build_r1_postgres_fetched_schema
        ),
    )
    boundary = runtime.prepare_boundary(connector=connector, lifecycle=lifecycle, load_config=_load_config())
    if terminal == "completed":
        lifecycle.complete()
    else:
        primary = RuntimeError("primary")
        caught = None
        try:
            boundary.abort_preserving(primary)
        except BaseException as error:
            caught = error
        if caught is not None:
            assert caught is primary

    with pytest.raises(Exception) as raised:
        boundary.require_active_for_copy(connector)
    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"


_V12_QUERY_PROFILE_GOLDEN: dict[str, tuple[tuple[tuple[str, str, bool], ...], str]] = {
    "initial_snapshot_witness": (
        (
            ("snapshot_token", "str", False),
            ("visible_horizon", "int", False),
            ("transaction_incarnation", "str", True),
            ("isolation_level", "str", False),
            ("read_only", "str", False),
            ("backend_pid", "int", False),
            ("database_name", "str", False),
            ("database_oid", "int", False),
            ("effective_principal", "str", False),
            ("effective_principal_oid", "int", False),
            ("session_principal", "str", False),
            ("session_principal_oid", "int", False),
            ("in_recovery", "bool", False),
            ("namespace_oid", "int", True),
            ("schema_name", "str", True),
            ("relation_oid", "int", True),
            ("relation_name", "str", True),
            ("lock_witness_count", "int", False),
        ),
        "exactly_one",
    ),
    "v1_physical_identity": (
        (("system_identifier", "str", False), ("timeline_id", "int", False)),
        "exactly_one",
    ),
    "active_scope_revalidation": (
        (
            ("snapshot_token", "str", False),
            ("transaction_incarnation", "str", True),
            ("lock_witness_count", "int", False),
        ),
        "exactly_one",
    ),
    "relation_profile": (
        (
            ("relation_oid", "int", False),
            ("namespace_oid", "int", False),
            ("schema_name", "str", False),
            ("relation_name", "str", False),
            ("relkind", "str", False),
            ("relpersistence", "str", False),
            ("relhassubclass", "bool", False),
        ),
        "exactly_one",
    ),
    "column_catalog": (
        (
            ("relation_oid", "int", False),
            ("namespace_oid", "int", False),
            ("relkind", "str", False),
            ("relpersistence", "str", False),
            ("relhassubclass", "bool", False),
            ("attribute_number", "int", False),
            ("column_name", "str", False),
            ("type_oid", "int", False),
            ("type_namespace_oid", "int", False),
            ("type_namespace_name", "str", False),
            ("type_name", "str", False),
            ("type_kind", "str", False),
            ("type_modifier", "int", False),
            ("nullable", "bool", False),
            ("collation_oid", "int", False),
            ("generated_kind", "str", False),
            ("identity_kind", "str", False),
        ),
        "zero_to_1025",
    ),
}

_V12_ALL_DATABASE_PHASES = (
    "set_transaction",
    "set_lock_timeout",
    "lock_relation",
    "initial_snapshot_witness",
    "v1_physical_identity",
    "active_scope_revalidation",
    "relation_profile",
    "column_catalog",
)
_V12_RELATION_DATABASE_PHASES = (
    "lock_relation",
    "initial_snapshot_witness",
    "active_scope_revalidation",
    "relation_profile",
    "column_catalog",
)

# Deliberately hand-written independently of the production query-profile
# builder.  These are protocol inputs, not values inferred from candidate SQL.
_V12_LITERAL_CATALOG_ROWS: dict[str, dict[str, Any]] = {
    "initial_snapshot_witness": {
        "snapshot_token": "10:20:",
        "visible_horizon": 20,
        "transaction_incarnation": "9/41",
        "isolation_level": "repeatable read",
        "read_only": "on",
        "backend_pid": 771,
        "database_name": "warehouse",
        "database_oid": 16384,
        "effective_principal": "dpone_reader",
        "effective_principal_oid": 17001,
        "session_principal": "dpone_login",
        "session_principal_oid": 17002,
        "in_recovery": False,
        "namespace_oid": 2200,
        "schema_name": "sales",
        "relation_oid": 22001,
        "relation_name": "orders",
        "lock_witness_count": 1,
    },
    "v1_physical_identity": {
        "system_identifier": "7272727272727272727",
        "timeline_id": 7,
    },
    "active_scope_revalidation": {
        "snapshot_token": "10:20:",
        "transaction_incarnation": "9/41",
        "lock_witness_count": 1,
    },
    "relation_profile": {
        "relation_oid": 22001,
        "namespace_oid": 2200,
        "schema_name": "sales",
        "relation_name": "orders",
        "relkind": "r",
        "relpersistence": "p",
        "relhassubclass": False,
    },
    "column_catalog": {
        "relation_oid": 22001,
        "namespace_oid": 2200,
        "relkind": "r",
        "relpersistence": "p",
        "relhassubclass": False,
        "attribute_number": 1,
        "column_name": "column_1",
        "type_oid": 20,
        "type_namespace_oid": 11,
        "type_namespace_name": "pg_catalog",
        "type_name": "int8",
        "type_kind": "b",
        "type_modifier": -1,
        "nullable": False,
        "collation_oid": 0,
        "generated_kind": "",
        "identity_kind": "",
    },
}


def _v12_statement_id(sql: str) -> str:
    if sql.startswith("SET TRANSACTION"):
        return "set_transaction"
    if sql.startswith("SET LOCAL lock_timeout"):
        return "set_lock_timeout"
    if sql.startswith("LOCK TABLE ONLY"):
        return "lock_relation"
    if "snapshot_witness AS MATERIALIZED" in sql:
        return "initial_snapshot_witness"
    if "pg_control_system" in sql:
        return "v1_physical_identity"
    if "txid_current_snapshot" in sql and "lock_witness_count" in sql:
        return "active_scope_revalidation"
    if "pg_attribute" in sql:
        return "column_catalog"
    if "c.relhassubclass" in sql:
        return "relation_profile"
    return "unknown"


class _V12CatalogProbeConnector(FakeCatalogConnector):
    def __init__(self) -> None:
        super().__init__()
        self.row_overrides: dict[str, list[dict[str, Any]]] = {}
        self.failure_phase: str | None = None
        self.failure_sqlstate: str | None = None
        self.statement_transcript: list[str] = []

    def execute_query(self, query: Any, params: tuple[object, ...] = ()) -> int:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.events.append(("execute", text, tuple(params)))
        self._raise_phase_failure(text)
        return 0

    def get_records(self, query: Any, params: tuple[object, ...] = (), as_dict: bool = False) -> list[Any]:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.events.append(("records", text, tuple(params)))
        self._raise_phase_failure(text)
        rows = self._rows(text)
        if as_dict is not True:
            raise AssertionError("catalog probes require mapping rows")
        return rows

    def _rows(self, sql: str) -> list[dict[str, Any]]:
        statement_id = _v12_statement_id(sql)
        if statement_id in self.row_overrides:
            return [dict(row) for row in self.row_overrides[statement_id]]
        literal = _V12_LITERAL_CATALOG_ROWS.get(statement_id)
        if literal is None:
            raise AssertionError(f"unexpected catalog statement: {statement_id}")
        return [dict(literal)]

    def _raise_phase_failure(self, sql: str) -> None:
        statement_id = _v12_statement_id(sql)
        self.statement_transcript.append(statement_id)
        if statement_id == self.failure_phase and self.failure_sqlstate is not None:
            # The exact reached statement is recorded immediately before the
            # injected driver failure.  A failure in an earlier phase cannot
            # satisfy a requested later-phase probe.
            assert self.statement_transcript[-1] == self.failure_phase
            raise FakeDriverError(self.failure_sqlstate)


def _v12_modules() -> dict[str, Any]:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    return _feature_modules()


def _v12_profile_item(modules: dict[str, Any], statement_id: str) -> Any:
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = issuer.bind_query_profile(selected_source_authority=selected)
    return next(item for item in profile.ordered_items if item.statement_id == statement_id)


def _v12_observe_catalog_call(
    modules: dict[str, Any],
    *,
    row_overrides: dict[str, list[dict[str, Any]]] | None = None,
    failure_phase: str | None = None,
    failure_sqlstate: str | None = None,
) -> tuple[str, str | None, str | None, bool, bool, str | None, str | None, tuple[str, ...]]:
    connector = _V12CatalogProbeConnector()
    connector.row_overrides.update(row_overrides or {})
    connector.failure_phase = failure_phase
    connector.failure_sqlstate = failure_sqlstate
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = issuer.bind_query_profile(selected_source_authority=selected)
    scope = None
    try:
        scope = (
            modules["snapshot"]
            .PostgresVerifiedRelationSnapshotIssuerV1()
            .open(
                connector=connector,
                lifecycle=ExtractionLifecycleAuthority(),
                selected_source_authority=selected,
                query_profile=profile.generic_relation_profile,
            )
        )
        issuer.issue(
            connector=connector,
            verified_relation=scope.require_active(connector),
            query_profile=profile,
        )
    except BaseException as error:
        recovery = getattr(getattr(error, "recovery", None), "value", None)
        return (
            "error",
            getattr(error, "reason", None),
            recovery,
            error.__cause__ is None,
            error.__context__ is None,
            failure_phase,
            connector.statement_transcript[-1] if connector.statement_transcript else None,
            tuple(connector.statement_transcript),
        )
    finally:
        if scope is not None:
            try:
                scope.close_if_active()
            except BaseException:
                pass
    return (
        "success",
        None,
        None,
        True,
        True,
        failure_phase,
        connector.statement_transcript[-1] if connector.statement_transcript else None,
        tuple(connector.statement_transcript),
    )


@pytest.mark.parametrize("statement_id", tuple(_V12_QUERY_PROFILE_GOLDEN))
def test_v12_executed_catalog_rows_obey_literal_closed_query_profile(statement_id: str) -> None:
    modules = _v12_modules()
    item = _v12_profile_item(modules, statement_id)
    literal_fields, literal_cardinality = _V12_QUERY_PROFILE_GOLDEN[statement_id]
    first = dict(_V12_LITERAL_CATALOG_ROWS[statement_id])
    expected_low = {
        "initial_snapshot_witness": "source_authority_mismatch",
        "v1_physical_identity": "source_authority_mismatch",
        "active_scope_revalidation": "snapshot_lease_mismatch",
        "relation_profile": "source_authority_mismatch",
        "column_catalog": "column_count_invalid",
    }[statement_id]
    expected_high = "column_count_invalid" if statement_id == "column_catalog" else "internal_invariant_violation"
    wrong_type_field = next(
        (name for name, python_type, _nullable in literal_fields if python_type == "bool"),
        next(name for name, python_type, _nullable in literal_fields if python_type == "int"),
    )
    wrong_type_row = dict(first)
    wrong_type_row[wrong_type_field] = True if type(first[wrong_type_field]) is int else 1
    extra_row = {**first, "unexpected_alias": "forbidden"}
    missing_row = dict(first)
    missing_row.pop(literal_fields[0][0])
    high_rows = [dict(first) for _ in range(1025 if statement_id == "column_catalog" else 2)]
    observed = [
        (
            tuple((field.name, field.python_type, field.nullable) for field in item.result_fields),
            item.cardinality,
        ),
        _v12_observe_catalog_call(modules, row_overrides={statement_id: [extra_row]})[1],
        _v12_observe_catalog_call(modules, row_overrides={statement_id: [missing_row]})[1],
        _v12_observe_catalog_call(modules, row_overrides={statement_id: [wrong_type_row]})[1],
        _v12_observe_catalog_call(modules, row_overrides={statement_id: []})[1],
        _v12_observe_catalog_call(modules, row_overrides={statement_id: high_rows})[1],
    ]
    expected: list[object] = [
        (literal_fields, literal_cardinality),
        "internal_invariant_violation",
        "internal_invariant_violation",
        "internal_invariant_violation",
        expected_low,
        expected_high,
    ]
    nullable_field = {
        "initial_snapshot_witness": "namespace_oid",
        "active_scope_revalidation": "transaction_incarnation",
    }.get(statement_id)
    if nullable_field is not None:
        nullable_row = dict(first)
        nullable_row[nullable_field] = None
        observed.append(_v12_observe_catalog_call(modules, row_overrides={statement_id: [nullable_row]})[1])
        expected.append(
            "snapshot_lease_mismatch" if statement_id == "active_scope_revalidation" else "source_authority_mismatch"
        )

    assert observed == expected


@pytest.mark.parametrize(
    "sqlstate",
    ("08006", "08999", "40001", "40P01", "40999", "53100", "53999", "55P03", "57014", "57P01", "57P02", "57P03"),
)
def test_v12_retryable_sqlstate_matrix_is_closed_across_every_database_phase(sqlstate: str) -> None:
    modules = _v12_modules()
    observed = []
    for phase in _V12_ALL_DATABASE_PHASES:
        result = _v12_observe_catalog_call(modules, failure_phase=phase, failure_sqlstate=sqlstate)
        observed.append(result)
    assert observed == [
        (
            "error",
            "catalog_observation_failed",
            "retryable_source",
            True,
            True,
            phase,
            phase,
            _V12_ALL_DATABASE_PHASES[: _V12_ALL_DATABASE_PHASES.index(phase) + 1],
        )
        for phase in _V12_ALL_DATABASE_PHASES
    ]


@pytest.mark.parametrize("sqlstate", ("42P01", "42704"))
def test_v12_missing_relation_sqlstate_is_operator_owned_across_relation_phases(sqlstate: str) -> None:
    modules = _v12_modules()
    observed = []
    for phase in _V12_RELATION_DATABASE_PHASES:
        result = _v12_observe_catalog_call(modules, failure_phase=phase, failure_sqlstate=sqlstate)
        observed.append(result)
    assert observed == [
        (
            "error",
            "source_authority_mismatch",
            "operator_intervention",
            True,
            True,
            phase,
            phase,
            _V12_ALL_DATABASE_PHASES[: _V12_ALL_DATABASE_PHASES.index(phase) + 1],
        )
        for phase in _V12_RELATION_DATABASE_PHASES
    ]


@pytest.mark.parametrize(
    ("facet", "mutations"),
    (
        ("relation_and_type_name", (("relation_profile", "relation_name", 7), ("column_catalog", "type_name", 7))),
        ("oid", (("relation_profile", "relation_oid", True), ("column_catalog", "type_oid", True))),
        ("typmod", (("column_catalog", "type_modifier", "-1"),)),
        ("nullable", (("column_catalog", "nullable", 0),)),
        ("collation", (("column_catalog", "collation_oid", False),)),
        ("relkind", (("relation_profile", "relkind", 1), ("column_catalog", "relkind", 1))),
    ),
)
def test_v12_malformed_catalog_identity_uses_structural_failure_precedence(
    facet: str,
    mutations: tuple[tuple[str, str, object], ...],
) -> None:
    modules = _v12_modules()
    observed: list[tuple[str, str | None, str | None, bool, bool, str | None, str | None, tuple[str, ...]]] = []
    for statement_id, field, bad_value in mutations:
        row = dict(_V12_LITERAL_CATALOG_ROWS[statement_id])
        row[field] = bad_value
        observed.append(_v12_observe_catalog_call(modules, row_overrides={statement_id: [row]}))

    assert (facet, observed) == (
        facet,
        [
            (
                "error",
                "internal_invariant_violation",
                "operator_intervention",
                True,
                True,
                None,
                "column_catalog",
                _V12_ALL_DATABASE_PHASES,
            )
            if statement_id == "column_catalog"
            else (
                "error",
                "internal_invariant_violation",
                "operator_intervention",
                True,
                True,
                None,
                "relation_profile",
                _V12_ALL_DATABASE_PHASES[:-1],
            )
            for statement_id, _field, _bad_value in mutations
        ],
    )
