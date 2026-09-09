from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.contracts.postgres_source_authority import (
    PostgresSourceAuthority,
    PostgresSourceAuthorityContractError,
    ascii_case_alias,
)
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.sources.postgres_source_authority import (
    PostgresSourceAuthorityVerificationError,
    PostgresSourceAuthorityVerifier,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    issue_repeatable_read_snapshot_lease,
)


def test_selected_digest_binds_only_one_route_relation() -> None:
    authority = PostgresSourceAuthority.from_connection_properties(
        _properties(
            relations={
                "public.events": _relation("public", "events", 2200, 16390),
                "public.unrelated": _relation("public", "unrelated", 2200, 16391),
            }
        )
    )
    first = authority.select(authored_schema="public", authored_relation="events")
    expanded = PostgresSourceAuthority.from_connection_properties(
        _properties(
            relations={
                "public.events": _relation("public", "events", 2200, 16390),
                "public.unrelated": _relation("public", "unrelated", 2200, 99999),
                "audit.runs": _relation("audit", "runs", 4200, 4201),
            }
        )
    ).select(authored_schema="public", authored_relation="events")

    assert first.authority_sha256 == expanded.authority_sha256
    assert "unrelated" not in str(first.to_document())


def test_version_one_selected_digest_remains_byte_compatible() -> None:
    selected = PostgresSourceAuthority.from_connection_properties(_properties()).select(
        authored_schema="public",
        authored_relation="events",
    )

    assert selected.authority_sha256 == "sha256:4b83922181905f80074ef1b71bca770ced8865cc033da72411e104c85d4c71f9"


def test_catalog_profile_has_distinct_truthful_route_identity() -> None:
    physical = PostgresSourceAuthority.from_connection_properties(_properties()).select(
        authored_schema="public",
        authored_relation="events",
    )
    catalog = PostgresSourceAuthority.from_connection_properties(_catalog_properties()).select(
        authored_schema="public",
        authored_relation="events",
    )

    assert catalog.authority_sha256 != physical.authority_sha256
    assert catalog.to_document()["verification_profile"] == "catalog_identity"
    assert "system_identifier" not in catalog.to_document()
    assert "timeline_id" not in catalog.to_document()


def test_ascii_case_alias_selects_one_exact_canonical_pin() -> None:
    selected = PostgresSourceAuthority.from_connection_properties(_properties()).select(
        authored_schema="PUBLIC",
        authored_relation="Events",
    )

    assert selected.relation.schema == "public"
    assert selected.relation.relation == "events"
    assert selected.authored_uses_case_alias is True


@pytest.mark.parametrize(
    ("authored", "canonical", "expected"),
    (
        ("Events", "events", True),
        ("Évents", "Évents", True),
        ("Évents", "évents", False),
        ("事件", "事件", True),
        ("事件A", "事件a", True),
    ),
)
def test_ascii_case_alias_never_folds_unicode(
    authored: str,
    canonical: str,
    expected: bool,
) -> None:
    assert ascii_case_alias(authored, canonical) is expected


def test_registry_rejects_duplicate_ascii_fold_relation_keys() -> None:
    with pytest.raises(
        PostgresSourceAuthorityContractError,
        match="postgres_source_relation_authority_ascii_case_ambiguous",
    ):
        PostgresSourceAuthority.from_connection_properties(
            _properties(
                relations={
                    "public.events": _relation("public", "events", 2200, 16390),
                    "PUBLIC.Events": _relation("PUBLIC", "Events", 2201, 16391),
                }
            )
        )


def test_generated_connection_registry_schema_accepts_closed_source_authority() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    payload = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "source": {
                "type": "postgres",
                "connection": _properties(),
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "postgres_source",
                    "execution_mode": "operator_bridge",
                },
            }
        },
    }

    jsonschema.validate(payload, schema)
    payload["connections"]["source"]["connection"] = _catalog_properties()
    jsonschema.validate(payload, schema)
    payload["connections"]["source"]["connection"]["postgres_source_authority"]["extra"] = True
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


@pytest.mark.parametrize(
    ("mutate", "code"),
    (
        (
            lambda document: document["postgres_source_authority"].update(extra=True),
            "postgres_source_authority_fields_invalid",
        ),
        (
            lambda document: document["postgres_source_authority"].update(version=3),
            "postgres_source_authority_version_unsupported",
        ),
        (
            lambda document: document["postgres_source_authority"].update(system_identifier="-1"),
            "postgres_source_system_identifier_invalid",
        ),
        (
            lambda document: document["postgres_source_authority"].update(timeline_id=True),
            "postgres_source_timeline_id_invalid",
        ),
        (
            lambda document: document["postgres_source_authority"]["database"].update(extra=True),
            "postgres_source_database_fields_invalid",
        ),
        (
            lambda document: document["postgres_source_authority"]["relations"]["public.events"].update(relation_oid=0),
            "source_relation_oid_invalid",
        ),
        (
            lambda document: document["postgres_source_authority"]["relations"].update(
                {"public.other": _relation("public", "events", 2200, 16390)}
            ),
            "source_relation_authority_key_not_canonical",
        ),
    ),
)
def test_authority_document_is_closed_and_typed(
    mutate: Any,
    code: str,
) -> None:
    properties = deepcopy(_properties())
    mutate(properties)

    with pytest.raises(PostgresSourceAuthorityContractError, match=code):
        PostgresSourceAuthority.from_connection_properties(properties)


def test_missing_route_relation_fails_without_learning() -> None:
    authority = PostgresSourceAuthority.from_connection_properties(_properties())

    with pytest.raises(
        PostgresSourceAuthorityContractError,
        match="postgres_source_relation_authority_missing",
    ):
        authority.select(authored_schema="public", authored_relation="missing")


def test_catalog_profile_is_closed_and_cannot_carry_unverified_physical_pins() -> None:
    properties = _catalog_properties()
    properties["postgres_source_authority"]["system_identifier"] = "761991928213"

    with pytest.raises(
        PostgresSourceAuthorityContractError,
        match="postgres_source_authority_fields_invalid",
    ):
        PostgresSourceAuthority.from_connection_properties(properties)


def test_catalog_profile_rejects_unknown_verification_profile() -> None:
    properties = _catalog_properties()
    properties["postgres_source_authority"]["verification_profile"] = "best_effort"

    with pytest.raises(
        PostgresSourceAuthorityContractError,
        match="postgres_source_verification_profile_invalid",
    ):
        PostgresSourceAuthority.from_connection_properties(properties)


def test_rr_verifier_proves_exact_identity_and_canonicalizes_ascii_alias() -> None:
    connector = _Connector()
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection())
    config = _config(schema="PUBLIC", relation="Events")
    expected = verifier.preflight(config)

    observed = verifier.verify_snapshot(
        connector=connector,
        snapshot_lease=_lease(connector),
        load_config=config,
    )

    assert expected.to_dict() == observed.to_dict()
    assert config.source_schema == "public"
    assert config.source_table == "events"
    assert observed.timeline_id == 1
    assert observed.diagnostics() == {
        "server_address": "192.0.2.8",
        "server_port": 5432,
    }
    assert "server_address" not in observed.to_dict()
    assert connector.lock_calls == 1


def test_catalog_profile_never_queries_cluster_control_or_statistics() -> None:
    connector = _Connector(
        identity_overrides={"in_recovery": True},
        reject_privileged_queries=True,
    )
    verifier = PostgresSourceAuthorityVerifier.from_connection(
        _connection(verification_profile="catalog_identity", topology_role="standby")
    )
    config = _config()

    expected = verifier.preflight(config)
    observed = verifier.verify_snapshot(
        connector=connector,
        snapshot_lease=_lease(connector),
        load_config=config,
    )

    assert observed.to_dict() == expected.to_dict()
    assert observed.version == 3
    assert observed.cluster_identifier is None
    assert observed.timeline_id is None
    assert observed.verification_profile == "catalog_identity"
    assert connector.queried_cluster_control is False
    assert connector.queried_wal_receiver is False


@pytest.mark.parametrize(
    ("field", "value", "code"),
    (
        ("database_oid", 999, "database_identity_mismatch"),
        ("effective_principal_oid", 999, "effective_principal_identity_mismatch"),
        ("session_principal_oid", 999, "session_principal_identity_mismatch"),
        ("in_recovery", False, "topology_role_mismatch"),
    ),
)
def test_catalog_profile_rejects_each_declared_identity_drift(
    field: str,
    value: Any,
    code: str,
) -> None:
    connector = _Connector(
        identity_overrides={"in_recovery": True, field: value},
        reject_privileged_queries=True,
    )
    verifier = PostgresSourceAuthorityVerifier.from_connection(
        _connection(verification_profile="catalog_identity", topology_role="standby")
    )

    with pytest.raises(PostgresSourceAuthorityVerificationError, match=code):
        verifier.verify_snapshot(
            connector=connector,
            snapshot_lease=_lease(connector),
            load_config=_config(),
        )


def test_rr_verifier_rechecks_oid_after_relation_lock() -> None:
    connector = _Connector(relation_after_lock_overrides={"relation_oid": 999})
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection())

    with pytest.raises(
        PostgresSourceAuthorityVerificationError,
        match="relation_identity_mismatch",
    ):
        verifier.verify_snapshot(
            connector=connector,
            snapshot_lease=_lease(connector),
            load_config=_config(),
        )

    assert connector.lock_calls == 1


@pytest.mark.parametrize(
    ("field", "value", "code"),
    (
        ("system_identifier", "999", "system_identifier_mismatch"),
        ("timeline_id", 2, "timeline_id_mismatch"),
        ("database_oid", 999, "database_identity_mismatch"),
        ("effective_principal_oid", 999, "effective_principal_identity_mismatch"),
        ("session_principal_oid", 999, "session_principal_identity_mismatch"),
        ("in_recovery", True, "topology_role_mismatch"),
    ),
)
def test_rr_verifier_rejects_each_identity_drift(
    field: str,
    value: Any,
    code: str,
) -> None:
    connector = _Connector(identity_overrides={field: value})
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection())

    with pytest.raises(PostgresSourceAuthorityVerificationError, match=code):
        verifier.verify_snapshot(
            connector=connector,
            snapshot_lease=_lease(connector),
            load_config=_config(),
        )


def test_rr_verifier_rejects_relation_oid_recreate() -> None:
    connector = _Connector(relation_overrides={"relation_oid": 999})
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection())

    with pytest.raises(
        PostgresSourceAuthorityVerificationError,
        match="relation_identity_mismatch",
    ):
        verifier.verify_snapshot(
            connector=connector,
            snapshot_lease=_lease(connector),
            load_config=_config(),
        )


def test_rr_verifier_rejects_ascii_fold_catalog_decoy() -> None:
    connector = _Connector(
        extra_relations=(
            {
                "namespace_oid": 2201,
                "schema_name": "PUBLIC",
                "relation_oid": 16391,
                "relation_name": "Events",
            },
        )
    )
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection())

    with pytest.raises(
        PostgresSourceAuthorityVerificationError,
        match="relation_ascii_case_ambiguous",
    ):
        verifier.verify_snapshot(
            connector=connector,
            snapshot_lease=_lease(connector),
            load_config=_config(schema="PUBLIC", relation="Events"),
        )


def test_standby_timeline_uses_control_checkpoint_without_stats_visibility() -> None:
    connector = _Connector(
        identity_overrides={"in_recovery": True},
        checkpoint_rows=({"timeline_id": 1},),
    )
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection(topology_role="standby"))

    identity = verifier.verify_snapshot(
        connector=connector,
        snapshot_lease=_lease(connector),
        load_config=_config(),
    )

    assert identity.timeline_id == 1
    assert connector.queried_wal_receiver is False


@pytest.mark.parametrize("checkpoint_rows", ((), ({"timeline_id": None},)))
def test_standby_timeline_rejects_unavailable_control_checkpoint(
    checkpoint_rows: tuple[dict[str, Any], ...],
) -> None:
    connector = _Connector(
        identity_overrides={"in_recovery": True},
        checkpoint_rows=checkpoint_rows,
    )
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection(topology_role="standby"))

    with pytest.raises(
        PostgresSourceAuthorityVerificationError,
        match="timeline_unavailable",
    ):
        verifier.verify_snapshot(
            connector=connector,
            snapshot_lease=_lease(connector),
            load_config=_config(),
        )


def test_rr_verifier_types_only_postgres_permission_denial() -> None:
    denied = _Connector(error=_SqlstateError("42501"))
    unavailable = _Connector(error=ConnectionError("wire closed"))
    verifier = PostgresSourceAuthorityVerifier.from_connection(_connection())

    with pytest.raises(
        PostgresSourceAuthorityVerificationError,
        match="metadata_permission_denied",
    ):
        verifier.verify_snapshot(
            connector=denied,
            snapshot_lease=_lease(denied),
            load_config=_config(),
        )
    with pytest.raises(ConnectionError, match="wire closed"):
        verifier.verify_snapshot(
            connector=unavailable,
            snapshot_lease=_lease(unavailable),
            load_config=_config(),
        )


def _properties(
    *,
    relations: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "database": "dpone_it",
        "postgres_source_authority": {
            "version": 1,
            "system_identifier": "761991928213",
            "timeline_id": 1,
            "topology_role": "primary",
            "database": {"canonical_name": "dpone_it", "oid": 16384},
            "principals": {
                "effective": {"canonical_name": "dpone", "oid": 16385},
                "session": {"canonical_name": "dpone", "oid": 16385},
            },
            "relations": relations
            or {
                "public.events": _relation("public", "events", 2200, 16390),
            },
        },
    }


def _catalog_properties(
    *,
    relations: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "database": "dpone_it",
        "postgres_source_authority": {
            "version": 2,
            "verification_profile": "catalog_identity",
            "topology_role": "standby",
            "database": {"canonical_name": "dpone_it", "oid": 16384},
            "principals": {
                "effective": {"canonical_name": "dpone", "oid": 16385},
                "session": {"canonical_name": "dpone", "oid": 16385},
            },
            "relations": relations
            or {
                "public.events": _relation("public", "events", 2200, 16390),
            },
        },
    }


def _connection(
    *,
    topology_role: str = "primary",
    verification_profile: str = "physical_cluster",
) -> ResolvedBindingConnection:
    properties = _properties() if verification_profile == "physical_cluster" else _catalog_properties()
    properties["postgres_source_authority"]["topology_role"] = topology_role
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database="dpone_it"),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="postgres",
            properties=properties,
        ),
    )


def _config(
    *,
    schema: str = "public",
    relation: str = "events",
) -> SimpleNamespace:
    return SimpleNamespace(source_schema=schema, source_table=relation)


def _lease(connector: Any):
    return issue_repeatable_read_snapshot_lease(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        raw_snapshot_token="100:100:200",
        visible_horizon=200,
    )


class _SqlstateError(PermissionError):
    def __init__(self, sqlstate: str) -> None:
        super().__init__("vendor denied")
        self.sqlstate = sqlstate


class _Connector:
    def __init__(
        self,
        *,
        identity_overrides: dict[str, Any] | None = None,
        relation_overrides: dict[str, Any] | None = None,
        relation_after_lock_overrides: dict[str, Any] | None = None,
        extra_relations: tuple[dict[str, Any], ...] = (),
        checkpoint_rows: tuple[dict[str, Any], ...] | None = None,
        error: Exception | None = None,
        reject_privileged_queries: bool = False,
    ) -> None:
        self.connection = object()
        self.identity_overrides = identity_overrides or {}
        self.relation_overrides = relation_overrides or {}
        self.relation_after_lock_overrides = relation_after_lock_overrides or {}
        self.extra_relations = extra_relations
        self.checkpoint_rows = checkpoint_rows
        self.error = error
        self.lock_calls = 0
        self.queried_wal_receiver = False
        self.queried_cluster_control = False
        self.reject_privileged_queries = reject_privileged_queries

    def execute_query(self, query: Any, params: Any = None) -> int:
        assert params is None
        assert "ACCESS SHARE MODE" in str(query)
        self.lock_calls += 1
        return 0

    def get_records(
        self,
        query: str,
        params: Any = None,
        *,
        as_dict: bool,
    ) -> list[dict[str, Any]]:
        assert as_dict
        if self.error is not None:
            raise self.error
        if "pg_control_system" in query:
            self.queried_cluster_control = True
            if self.reject_privileged_queries:
                raise AssertionError("catalog identity must not query cluster-control functions")
            row = {
                "system_identifier": "761991928213",
                "database_name": "dpone_it",
                "database_oid": 16384,
                "effective_principal": "dpone",
                "effective_principal_oid": 16385,
                "session_principal": "dpone",
                "session_principal_oid": 16385,
                "in_recovery": False,
                "server_address": "192.0.2.8",
                "server_port": 5432,
            }
            return [{**row, **self.identity_overrides}]
        if "current_database()" in query:
            row = {
                "database_name": "dpone_it",
                "database_oid": 16384,
                "effective_principal": "dpone",
                "effective_principal_oid": 16385,
                "session_principal": "dpone",
                "session_principal_oid": 16385,
                "in_recovery": False,
                "server_address": "192.0.2.8",
                "server_port": 5432,
            }
            return [{**row, **self.identity_overrides}]
        if "pg_stat_wal_receiver" in query:
            self.queried_wal_receiver = True
            raise AssertionError("standby identity must not require statistics views")
        if "pg_control_checkpoint" in query:
            self.queried_cluster_control = True
            if self.reject_privileged_queries:
                raise AssertionError("catalog identity must not query cluster-control functions")
            if self.checkpoint_rows is not None:
                return [dict(row) for row in self.checkpoint_rows]
            return [{"timeline_id": self.identity_overrides.get("timeline_id", 1)}]
        if "pg_walfile_name" in query:
            self.queried_cluster_control = True
            if self.reject_privileged_queries:
                raise AssertionError("catalog identity must not query WAL-control functions")
            return [{"timeline_id": self.identity_overrides.get("timeline_id", 1)}]
        assert params is not None
        row = {
            "namespace_oid": 2200,
            "schema_name": "public",
            "relation_oid": 16390,
            "relation_name": "events",
        }
        overrides = self.relation_after_lock_overrides if self.lock_calls else self.relation_overrides
        return [{**row, **overrides}, *self.extra_relations]


def _relation(
    schema: str,
    relation: str,
    namespace_oid: int,
    relation_oid: int,
) -> dict[str, Any]:
    return {
        "schema": schema,
        "relation": relation,
        "namespace_oid": namespace_oid,
        "relation_oid": relation_oid,
    }
