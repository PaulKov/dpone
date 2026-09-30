"""Synthetic storage-boundary contracts; these do not certify a live Keeper deployment."""

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.ports.clickhouse_cluster_publication import contracts
from dpone.runtime.sinks.clickhouse_quality_authority import ClickHouseQualityKeeperMapAuthority

HOSTS = ("replica-a", "replica-b")
COLUMNS = (
    ("target_key", "String"),
    ("operation_id", "String"),
    ("fence_token", "String"),
    ("phase", "String"),
    ("dispatch_epoch", "UInt64"),
    ("payload", "String"),
    ("payload_sha256", "FixedString(64)"),
)


def record() -> contracts.AuthorityRecord:
    return contracts.AuthorityRecord(
        target_key="target",
        operation_id="operation",
        fence_token="fence",
        phase=contracts.AuthorityPhase.PREPARED,
        dispatch_epoch=0,
        inventory_digest="inventory",
        plan_digest="plan",
        database="analytics",
        target="target",
        candidate="candidate",
        desired=contracts.GenerationIdentity("uuid", "ReplicatedMergeTree()", "schema", "keeper", "/data"),
        predecessor=None,
        staged_rows=2,
    )


class Connector:
    def __init__(self) -> None:
        self.tables = [(host, "KeeperMap('/dpone/authority')", "target_key") for host in HOSTS]
        self.columns = [
            (host, name, kind, index, "", "") for host in HOSTS for index, (name, kind) in enumerate(COLUMNS, 1)
        ]
        self.rows: list[Any] = []
        self.calls: list[Any] = []
        self.reads: list[str] = []
        self.error: Exception | None = None
        self.read_error: Exception | None = None
        self.connection = SimpleNamespace(execute=self.execute)

    def execute(self, sql: str, params: Any, **kwargs: Any) -> None:
        self.calls.append((sql, params, kwargs))
        if self.error:
            raise self.error
        if type(self) is Connector and self.rows:
            row = list(self.rows[0])
            payload = json.loads(row[4])
            payload["authority_write_id"] = json.loads(params["payload"])["authority_write_id"]
            row[4] = contracts.canonical_json(payload)
            row[5] = hashlib.sha256(row[4].encode()).hexdigest()
            self.rows = [tuple(row)]

    def get_records(self, sql: str, params: Any) -> list[Any]:
        self.reads.append(sql)
        if "system.tables" in sql:
            return self.tables
        if "system.columns" in sql:
            return self.columns
        if self.read_error:
            raise self.read_error
        return self.rows

    def observe(self, value: contracts.AuthorityRecord, version: Any) -> None:
        self.rows = [
            (
                value.operation_id,
                value.fence_token,
                value.phase.value,
                value.dispatch_epoch,
                value.payload,
                value.payload_sha256,
                version,
            )
        ]


def ready(connector: Connector) -> ClickHouseQualityKeeperMapAuthority:
    authority = ClickHouseQualityKeeperMapAuthority(connector, "analytics")
    authority.require_ready("cluster", "analytics", HOSTS)
    return authority


def test_unadmitted_keeper_has_no_linearizable_permit() -> None:
    authority = ClickHouseQualityKeeperMapAuthority(Connector(), "analytics")
    assert authority.supports_linearizable_dispatch_permit() is False


def test_admitted_keeper_has_linearizable_permit() -> None:
    authority = ready(Connector())
    assert authority.supports_linearizable_dispatch_permit() is True


def test_preflight_is_read_only_and_exact() -> None:
    connector = Connector()
    ready(connector)
    assert not connector.calls
    assert all(sql.startswith("SELECT") for sql in connector.reads)


@pytest.mark.parametrize("primary_key", ["target_key", "`target_key`"])
def test_admission_accepts_real_keeper_engine_full_primary_key_suffix(primary_key):
    connector = Connector()
    connector.tables = [
        (host, f"KeeperMap('/dpone/authority') PRIMARY KEY {primary_key}", primary_key) for host in HOSTS
    ]
    assert ready(connector).supports_linearizable_dispatch_permit()
    assert not connector.calls


def test_admission_rejects_extra_engine_expression_after_primary_key():
    connector = Connector()
    connector.tables = [
        (host, "KeeperMap('/dpone/authority') PRIMARY KEY target_key OTHER", "target_key") for host in HOSTS
    ]
    with pytest.raises(contracts.ClusterPublicationError, match="UNSUPPORTED"):
        ready(connector)


@pytest.mark.parametrize(
    "case", ["legacy", "absent", "missing_host", "duplicate_host", "path", "macro", "pk", "column", "extra", "default"]
)
def test_preflight_rejects_untrusted_storage_without_ddl(case: str) -> None:
    connector = Connector()
    if case == "legacy":
        connector.tables[0] = (HOSTS[0], "ReplicatedReplacingMergeTree()", "target_key")
    elif case == "absent":
        connector.tables = []
    elif case == "missing_host":
        connector.tables.pop()
    elif case == "duplicate_host":
        connector.tables.append(connector.tables[0])
    elif case == "path":
        connector.tables[0] = (HOSTS[0], "KeeperMap('/other')", "target_key")
    elif case == "macro":
        connector.tables = [(host, "KeeperMap('/{replica}/authority')", "target_key") for host in HOSTS]
    elif case == "pk":
        connector.tables[0] = (HOSTS[0], "KeeperMap('/dpone/authority')", "operation_id")
    elif case == "column":
        connector.columns[0] = (HOSTS[0], "target_key", "UInt64", 1, "", "")
    elif case == "extra":
        connector.columns.append((HOSTS[0], "version", "UInt64", 8, "", ""))
    else:
        connector.columns[0] = (HOSTS[0], "target_key", "String", 1, "DEFAULT", "'unsafe'")
    with pytest.raises(contracts.ClusterPublicationError, match="UNSUPPORTED"):
        ready(connector)
    assert not connector.calls


def test_unchecked_authority_cannot_write() -> None:
    connector = Connector()
    with pytest.raises(contracts.ClusterPublicationError, match="UNSUPPORTED"):
        ClickHouseQualityKeeperMapAuthority(connector, "analytics").create_if_absent(record())
    assert not connector.calls


def test_strict_insert_starts_at_zero_and_bypasses_generic_retry() -> None:
    connector = AtomicConnector()
    authority = ready(connector)
    desired = record()
    result = authority.create_if_absent(desired)
    assert result.status is contracts.AuthorityMutationStatus.VERIFIED
    assert result.permit is None
    sql, _, kwargs = connector.calls[0]
    assert sql.startswith("INSERT INTO") and "VALUES" in sql and "SELECT" not in sql
    assert kwargs["settings"] == {"keeper_map_strict_mode": 1, "insert_keeper_max_retries": 0}
    assert len(connector.calls) == 1


def test_cas_requires_version_operation_fence_phase_and_exact_readback() -> None:
    connector = Connector()
    authority = ready(connector)
    before = contracts.VersionedAuthorityRecord(record(), 3)
    desired = before.record.dispatching(token="token", query_digest="query")
    connector.observe(desired, 4)
    result = authority.compare_and_swap(before, desired)
    assert result.status is contracts.AuthorityMutationStatus.VERIFIED
    assert result.permit is not None
    sql, params, kwargs = connector.calls[0]
    assert sql.startswith("ALTER TABLE")
    for predicate in (
        "_version = %(version)s",
        "operation_id = %(expected_operation)s",
        "fence_token = %(expected_fence)s",
        "phase = %(expected_phase)s",
    ):
        assert predicate in sql
    assert params["version"] == 3
    assert kwargs["settings"]["insert_keeper_max_retries"] == 0


@pytest.mark.parametrize("failure", ["ack", "read", "missing", "stale", "payload"])
def test_uncertain_or_conflicting_mutation_never_issues_permit(failure: str) -> None:
    connector = Connector()
    authority = ready(connector)
    before = contracts.VersionedAuthorityRecord(record(), 3)
    desired = before.record.dispatching(token="token", query_digest="query")
    connector.observe(desired, 4)
    if failure == "ack":
        connector.error = TimeoutError("lost write acknowledgement")
    elif failure == "read":
        connector.read_error = TimeoutError("lost read acknowledgement")
    elif failure == "missing":
        connector.rows = []
    elif failure == "stale":
        connector.observe(desired, 3)
    else:
        connector.observe(replace(desired, plan_digest="different"), 4)
    result = authority.compare_and_swap(before, desired)
    assert result.status is not contracts.AuthorityMutationStatus.VERIFIED
    assert result.permit is None
    assert len(connector.calls) == 1


@pytest.mark.parametrize(
    "case",
    [
        "duplicate",
        "hash",
        "phase",
        "boolean_version",
        "negative_version",
        "float_version",
        "unknown_field",
        "duplicate_json_key",
    ],
)
def test_read_rejects_malformed_authority(case: str) -> None:
    connector = Connector()
    authority = ready(connector)
    connector.observe(record(), 0)
    row = list(connector.rows[0])
    if case == "duplicate":
        connector.rows *= 2
    elif case == "hash":
        row[5] = "0" * 64
    elif case == "phase":
        row[2] = "COMPLETED"
    elif case == "boolean_version":
        row[6] = True
    elif case == "negative_version":
        row[6] = -1
    elif case == "float_version":
        row[6] = 0.0
    elif case == "unknown_field":
        row[4] = row[4][:-1] + ',"unknown":1}'
    else:
        row[4] = row[4][:-1] + ',"staged_rows":2}'
    if case != "duplicate":
        connector.rows = [tuple(row)]
    with pytest.raises(contracts.ClusterPublicationError, match="INVALID"):
        authority.read_versioned("target")


def test_cas_cannot_move_target_key() -> None:
    connector = Connector()
    authority = ready(connector)
    with pytest.raises(contracts.ClusterPublicationError, match="INVALID"):
        authority.compare_and_swap(
            contracts.VersionedAuthorityRecord(record(), 0), replace(record(), target_key="other")
        )
    assert not connector.calls


class AtomicConnector(Connector):
    """Model Keeper's atomic versioned write and SQL's zero-row UPDATE distinction."""

    def execute(self, sql: str, params: Any, **kwargs: Any) -> None:
        super().execute(sql, params, **kwargs)
        if sql.startswith("INSERT"):
            if self.rows:
                raise RuntimeError("strict insert conflict")
            version = 0
        else:
            if not self.rows:
                return
            if self.rows[0][6] != params["version"]:
                return
            if self.rows[0][5] != params.get("expected_payload_sha256", self.rows[0][5]):
                return
            version = params["version"] + 1
        self.rows = [
            (
                params["operation_id"],
                params["fence_token"],
                params["phase"],
                params["dispatch_epoch"],
                params["payload"],
                params["payload_sha256"],
                version,
            )
        ]


def test_identical_competing_cas_cannot_issue_second_permit() -> None:
    connector = AtomicConnector()
    first, second = ready(connector), ready(connector)
    created = first.create_if_absent(record())
    assert created.status is contracts.AuthorityMutationStatus.VERIFIED
    assert created.observed is not None
    before = created.observed
    desired = before.record.dispatching(token="same-token", query_digest="same-query")
    winner = first.compare_and_swap(before, desired)
    loser = second.compare_and_swap(before, desired)
    assert winner.permit is not None
    assert loser.permit is None
    assert loser.status is not contracts.AuthorityMutationStatus.VERIFIED
    assert len(connector.calls) == 3
    first_payload = json.loads(connector.calls[1][1]["payload"])
    second_payload = json.loads(connector.calls[2][1]["payload"])
    assert first_payload["authority_write_id"] != second_payload["authority_write_id"]
    assert loser.status is contracts.AuthorityMutationStatus.CONFLICT


def test_duplicate_strict_insert_remains_unknown_without_dispatch() -> None:
    connector = AtomicConnector()
    authority = ready(connector)
    desired = record()
    assert authority.create_if_absent(desired).status is contracts.AuthorityMutationStatus.VERIFIED
    result = authority.create_if_absent(desired)
    assert result.status is contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN
    assert result.permit is None
    assert len(connector.calls) == 2


def test_legacy_payload_without_optional_nonce_remains_readable() -> None:
    connector = Connector()
    authority = ready(connector)
    old = record()
    connector.observe(old, 2)
    assert "authority_write_id" not in old.payload
    observed = authority.read_versioned(old.target_key)
    assert observed is not None
    assert observed.record.authority_write_id is None
    assert observed.version == 2


def test_failed_readmission_revokes_previous_write_capability() -> None:
    connector = Connector()
    authority = ready(connector)
    connector.tables = []
    with pytest.raises(contracts.ClusterPublicationError, match="UNSUPPORTED"):
        authority.require_ready("cluster", "analytics", HOSTS)
    with pytest.raises(contracts.ClusterPublicationError, match="UNSUPPORTED"):
        authority.create_if_absent(record())
    assert not connector.calls


def test_write_ack_loss_does_not_attempt_readback_or_grant_permission() -> None:
    connector = Connector()
    authority = ready(connector)
    connector.error = TimeoutError("write acknowledgement lost")
    reads_before = len(connector.reads)
    result = authority.create_if_absent(record())
    assert result.status is contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN
    assert result.permit is None
    assert len(connector.reads) == reads_before


def test_read_missing_key_is_absent_but_not_malformed() -> None:
    connector = Connector()
    assert ready(connector).read_versioned("target") is None


def test_cancellation_is_never_swallowed_or_retried() -> None:
    connector = Connector()
    authority = ready(connector)

    def cancel(*args: Any, **kwargs: Any) -> None:
        raise KeyboardInterrupt

    connector.connection.execute = cancel
    with pytest.raises(KeyboardInterrupt):
        authority.create_if_absent(record())


@pytest.mark.parametrize(
    "schema,evidence",
    [("unknown", None), (contracts.QUALITY_SCHEMA_VERSION, None), (contracts.SCHEMA_VERSION, "capsule")],
)
def test_schema_version_must_match_quality_envelope(schema: str, evidence: str | None) -> None:
    connector = Connector()
    authority = ready(connector)
    connector.observe(replace(record(), schema_version=schema, quality_evidence=evidence), 0)
    with pytest.raises(contracts.ClusterPublicationError, match="INVALID"):
        authority.read_versioned("target")


def test_v2_capsule_bytes_survive_create_and_cas() -> None:
    connector = AtomicConnector()
    authority = ready(connector)
    desired = replace(
        record(), schema_version=contracts.QUALITY_SCHEMA_VERSION, quality_evidence='{"opaque":"capsule"}'
    )
    created = authority.create_if_absent(desired)
    assert created.observed is not None
    assert created.observed.record.quality_evidence == desired.quality_evidence
    updated = authority.compare_and_swap(
        created.observed, created.observed.record.dispatching(token="token", query_digest="query")
    )
    assert updated.status is contracts.AuthorityMutationStatus.VERIFIED
    assert updated.observed is not None
    assert updated.observed.record.quality_evidence == desired.quality_evidence
    assert updated.observed.record.authority_write_id != created.observed.record.authority_write_id
