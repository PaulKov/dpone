"""Shared original-row reader regressions; doubles never certify live SQL."""

from __future__ import annotations

import re
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from dpone.adapters.composition_mssql_ownership import (
    SharedDomainRecord,
    SharedOwnerRecord,
    iter_shared_owners_in,
    read_shared_domain_in,
    read_shared_owner_in,
)
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.contracts.composition_activation import CompositionActivationRequest, CompositionAdmissionError
from dpone.contracts.composition_ownership import CompositionOwnerReference
from dpone.contracts.composition_persistence import encode_activation_request, encode_physical_resource
from dpone.contracts.composition_qualification_operation import CompositionQualificationOwner
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_mssql_store_helpers import SERVICE_ID, domain_guard, mixed_request
from tests.test_composition_activation_contract import digest
from tests.test_composition_qualification_operation import owner as qualification_owner


def test_read_records_are_immutable() -> None:
    record = SharedDomainRecord("guard", "mssql", "service", "physical", 0, None)
    with pytest.raises(FrozenInstanceError):
        record.fencing_epoch = 1  # type: ignore[misc]
    assert SharedOwnerRecord.__dataclass_params__.frozen


class SharedSql:
    """Bounded read-only DB-API model with independently mutable transaction state.

    Stored tuples mirror the frozen schema columns. Statement dispatch is only a
    unit fixture, not proof that SQL Server enforces these queries or constraints.
    """

    schema = "dpone_control"

    def __init__(self) -> None:
        self.cursor = self
        self.owners: dict[str, tuple[Any, ...]] = {}
        self.domains: dict[str, tuple[Any, ...]] = {}
        self.owner_domains: list[tuple[Any, ...]] = []
        self.operations: dict[str, tuple[Any, ...]] = {}
        self.operation_domains: list[tuple[Any, ...]] = []
        self.transaction = (1, 1, "Exclusive", 7)
        self.authority = [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE_ID)]
        self.results: list[tuple[Any, ...]] = []
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.after_execute: Any = None

    def table(self, name: str) -> str:
        raise AssertionError("Caller-supplied table names must not select authority")

    def add_owner(self, subject: Any = None, state: str = "ACTIVE") -> CompositionOwnerReference:
        subject = mixed_request() if subject is None else subject
        if type(subject) is CompositionActivationRequest:
            reference = CompositionOwnerReference("execution", subject.activation_id)
            resources = [(resource, encode_physical_resource(resource)) for resource in subject.resources]
            raw, sha = encode_activation_request(subject), subject.request_sha256
        else:
            reference = subject.owner_reference
            resources = [(claim, canonical_json_bytes(claim.to_dict())) for claim in subject.claims]
            raw, sha = subject.to_bytes(), subject.subject_sha256
        key = reference.owner_key
        self.owners[key] = (key, reference.owner_kind, reference.owner_id, sha, raw, state)
        for resource, document in resources:
            old = self.domains.get(resource.guard_id)
            epoch = 1 if old is None else old[3] + 1
            pointer = None if state in {"RETIRED", "TRANSFERRED"} else key
            self.domains[resource.guard_id] = (
                resource.connector,
                resource.service_id,
                resource.physical_subject_sha256,
                epoch,
                pointer,
            )
            self.owner_domains.append((key, resource.guard_id, document, epoch))
        return reference

    def execute(self, statement: str, *parameters: Any) -> SharedSql:
        sql = " ".join(statement.split())
        self.statements.append((sql, parameters))
        self.results = self._select(sql, parameters)
        if self.after_execute:
            self.after_execute(self, sql, parameters)
        return self

    def _select(self, sql: str, p: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if sql.startswith("DECLARE @count"):
            assert p == ("dpone:composition-control:v1",)
            return [self.transaction]
        assert sql.startswith("SELECT "), "Readers must issue only observations"
        if "composition_authority]" in sql:
            return list(self.authority)
        if "COUNT_BIG(*)" in sql:
            rows = [r for r in self.owner_domains if r[1] == p[0]]
            epochs = [r[3] for r in rows]
            return [
                (
                    len(rows),
                    min(epochs, default=None),
                    max(epochs, default=None),
                    sum(r[0] in self.owners for r in rows),
                )
            ]
        top_match = re.search(r"TOP \((\d+)\)", sql)
        assert top_match, "Every fetched row partition must have a SQL-side bound"
        top = int(top_match[1])
        if "SELECT TOP (1) p.guard_id" in sql:
            if "FROM [dpone_control].[composition_operation_domains]" in sql:
                return [
                    (r[2],)
                    for r in self.operation_domains
                    if r[0] not in self.operations
                    or (r[1], r[2], r[3]) not in {(d[0], d[1], d[3]) for d in self.owner_domains}
                    or self.operations[r[0]][2] != r[1]
                ][:top]
            return [(r[1],) for r in self.owner_domains if r[0] not in self.owners or r[1] not in self.domains][:top]
        if "SELECT TOP (1) d.guard_id" in sql:
            return [
                (guard,)
                for guard, row in self.domains.items()
                if row[3] > 0 and not any((r[1], r[3]) == (guard, row[3]) for r in self.owner_domains)
            ][:top]
        if "SELECT TOP (2) p.owner_key" in sql:
            return [
                (r[0], *self.owners.get(r[0], (None,) * 6)[1:3], self.owners.get(r[0], (None,) * 6)[5])
                for r in self.owner_domains
                if (r[1], r[3]) == p
            ][:top]
        if "FROM [dpone_control].[composition_domains]" in sql:
            return [self.domains[p[0]]] if p[0] in self.domains else []
        if "FROM [dpone_control].[composition_owner_domains]" in sql:
            if "claim_document" in sql:
                return [
                    (r[2] if len(r[2]) == p[0] else None,) for r in self.owner_domains if (r[0], r[1], r[3]) == p[1:]
                ][:top]
            return sorted((r[1], r[3]) for r in self.owner_domains if r[0] == p[0])[:top]
        if "FROM [dpone_control].[composition_operation_domains]" in sql:
            return sorted((r[1], r[2], r[3]) for r in self.operation_domains if r[0] == p[0])[:top]
        if "FROM [dpone_control].[composition_owners]" in sql:
            rows = list(self.owners.values())
            if "WHERE owner_kind='execution'" in sql:
                return [(r[0],) for r in rows if r[1] == "execution" and r[3] == p[0]][:top]
            if "WHERE owner_key>?" in sql:
                rows = [r for r in rows if r[0] > p[0]]
            elif "WHERE owner_key=? OR" in sql:
                rows = [
                    r
                    for r in rows
                    if r[0] == p[0] or (r[1] == p[1] and (r[2] == p[2] or (len(p) == 4 and r[3] == p[3])))
                ]
            elif "WHERE owner_key=?" in sql:
                rows = [r for r in rows if r[0] == p[0]]
            rows.sort(key=lambda r: r[0])
            if "SELECT TOP (2) owner_key FROM" in sql:
                return [(r[0],) for r in rows][:top]
            assert "DATALENGTH(subject_document) BETWEEN 1 AND" in sql
            return [self._bounded(r, 1, 4) for r in rows[:top]]
        if "FROM [dpone_control].[composition_operations]" in sql:
            rows = list(self.operations.values())
            if "WHERE operation_family=?" in sql:
                return [(r[0],) for r in rows if (r[1], r[4]) == p][:top]
            if "WHERE operation_key>?" in sql:
                rows = [r for r in rows if r[0] > p[0]]
            elif "WHERE operation_key=?" in sql:
                rows = [r for r in rows if r[0] == p[0]]
            assert "DATALENGTH(operation_document) BETWEEN 1 AND" in sql
            return [self._bounded(r, 1, 5) for r in sorted(rows, key=lambda r: r[0])[:top]]
        raise AssertionError(f"Unmodeled read: {sql}")

    @staticmethod
    def _bounded(row: tuple[Any, ...], family: int, document: int) -> tuple[Any, ...]:
        limit = {"execution": 8388608, "qualification": 1048576}.get(row[family], 0)
        raw = row[document]
        if isinstance(raw, bytes) and not 1 <= len(raw) <= limit:
            return (*row[:document], None, *row[document + 1 :])
        return row

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows, self.results = self.results, []
        return rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.results.pop(0) if self.results else None

    def close(self) -> None:
        raise AssertionError("A reader does not own cursor lifetime")


def test_complete_execution_original_and_partition_roundtrip() -> None:
    database = SharedSql()
    request = mixed_request()
    reference = database.add_owner(request)
    value = read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID)
    assert value is not None
    assert value.subject == request
    assert value.subject_document == encode_activation_request(request)
    assert value.guard_epochs == tuple((r.guard_id, 1) for r in request.resources)
    assert list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID)) == [value]
    for resource in request.resources:
        assert read_shared_domain_in(database, resource, expected_service_id=SERVICE_ID).owner == reference


def test_absent_owner_and_new_unowned_enrollment() -> None:
    database = SharedSql()
    request = mixed_request()
    reference = CompositionOwnerReference("execution", request.activation_id)
    assert read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID) is None
    resource = request.resources[0]
    database.domains[resource.guard_id] = (
        resource.connector,
        resource.service_id,
        resource.physical_subject_sha256,
        0,
        None,
    )
    assert read_shared_domain_in(database, resource, expected_service_id=SERVICE_ID).fencing_epoch == 0
    assert list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID)) == []


def test_same_uuid_different_owner_kinds_reopen_exact_originals() -> None:
    database = SharedSql()
    request = mixed_request()
    execution = database.add_owner(request)
    qualification = database.add_owner(qualification_owner(qualification_run_id=request.activation_id))
    assert execution.owner_id == qualification.owner_id
    assert execution.owner_key != qualification.owner_key
    records = list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID))
    assert {r.reference for r in records} == {execution, qualification}
    assert {type(r.subject) for r in records} == {CompositionActivationRequest, CompositionQualificationOwner}


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "SEALING", "SEALED", "TRANSFERRED", "RETIRED"])
def test_qualification_states_are_structural_records(state: str) -> None:
    database = SharedSql()
    original = qualification_owner()
    reference = database.add_owner(original, state)
    record = read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID)
    assert record is not None and record.subject_document == original.to_bytes() and record.state == state


@pytest.mark.parametrize("service", ["00000000-0000-0000-0000-000000000000", "11111111-1111-1111-8111-111111111111"])
def test_legacy_service_ids_and_backslash_unicode_bytes_remain_unchanged(service: str) -> None:
    original = mixed_request()
    resources = tuple(
        sorted(
            (
                replace(r, service_id=service, guard_id=domain_guard(r.connector, service, r.physical_subject_sha256))
                for r in original.resources
            ),
            key=lambda r: r.guard_id,
        )
    )
    workloads = tuple(replace(w, workload_id=w.workload_id + "\\данные") for w in original.workloads)
    request = replace(original, resources=resources, workloads=workloads)
    database = SharedSql()
    reference = database.add_owner(request)
    record = read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID)
    assert (
        record is not None
        and record.subject == request
        and record.subject_document == encode_activation_request(request)
    )
    assert b"\\\\" in record.subject_document
    for resource in request.resources:
        assert read_shared_domain_in(database, resource, expected_service_id=SERVICE_ID).service_id == service


@pytest.mark.parametrize(
    "damage,reason",
    [
        ("missing", "guard_partition"),
        ("extra", "guard_partition"),
        ("document", "guard_readback"),
        ("stale", "guard_readback"),
        ("overflow", "guard_readback"),
        ("boolean", "guard_readback"),
        ("physical", "domain_enrollment"),
        ("pointer", "guard_readback"),
    ],
)
def test_incomplete_or_changed_owner_partitions_reject(damage: str, reason: str) -> None:
    database = SharedSql()
    reference = database.add_owner()
    first = database.owner_domains[0]
    if damage == "missing":
        database.owner_domains.pop()
    elif damage == "extra":
        database.owner_domains.append((first[0], digest("extra guard"), b"{}", 1))
    elif damage == "document":
        database.owner_domains[0] = (*first[:2], first[2] + b" ", first[3])
    elif damage in {"stale", "overflow", "boolean"}:
        database.owner_domains[0] = (*first[:3], {"stale": 2, "overflow": 2**63, "boolean": True}[damage])
    else:
        domain = database.domains[first[1]]
        database.domains[first[1]] = ("postgres", *domain[1:]) if damage == "physical" else (*domain[:4], None)
    with pytest.raises(CompositionAdmissionError, match=reason):
        read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID)


@pytest.mark.parametrize("epoch", [-1, True, 2**63])
def test_domain_epoch_type_and_overflow_reject(epoch: Any) -> None:
    database = SharedSql()
    database.add_owner()
    resource = mixed_request().resources[0]
    row = database.domains[resource.guard_id]
    database.domains[resource.guard_id] = (*row[:3], epoch, row[4])
    with pytest.raises(CompositionAdmissionError, match="guard_epoch"):
        read_shared_domain_in(database, resource, expected_service_id=SERVICE_ID)


def test_retained_history_is_contiguous_and_latest_pointer_exact() -> None:
    database = SharedSql()
    first = mixed_request()
    retired = database.add_owner(first, "RETIRED")
    second = replace(first, context=replace(first.context, activation_id="55555555-5555-4555-8555-555555555555"))
    active = database.add_owner(second)
    assert read_shared_owner_in(database, retired, expected_service_id=SERVICE_ID).state == "RETIRED"
    assert read_shared_owner_in(database, active, expected_service_id=SERVICE_ID).guard_epochs == tuple(
        (r.guard_id, 2) for r in second.resources
    )
    database.owner_domains = [r for r in database.owner_domains if r[0] != retired.owner_key]
    database.owners.pop(retired.owner_key)
    with pytest.raises(CompositionAdmissionError, match="guard_readback"):
        read_shared_owner_in(database, active, expected_service_id=SERVICE_ID)


@pytest.mark.parametrize("bad_key", ["", "!", "sha256:" + "A" * 64])
def test_first_unfiltered_owner_page_rejects_malformed_low_keys(bad_key: str) -> None:
    database = SharedSql()
    reference = database.add_owner()
    row = database.owners.pop(reference.owner_key)
    database.owners[reference.owner_key] = (bad_key, *row[1:])
    with pytest.raises(CompositionAdmissionError, match="digest"):
        list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID))
    pages = [sql for sql, _ in database.statements if "ORDER BY owner_key;" in sql]
    assert pages and "WHERE" not in pages[0]


def test_absent_original_with_retained_partition_is_corruption() -> None:
    database = SharedSql()
    reference = database.add_owner()
    database.owners.clear()
    with pytest.raises(CompositionAdmissionError, match="guard_partition"):
        read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID)


def test_iterator_checks_transaction_after_actual_empty_end_page() -> None:
    database = SharedSql()

    def replace_transaction(context: SharedSql, sql: str, parameters: Any) -> None:
        if "ORDER BY owner_key;" in sql:
            context.transaction = (1, 1, "Exclusive", 8)

    database.after_execute = replace_transaction
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID))
