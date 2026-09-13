"""Trusted preplanning borrows the exact PG lease and owns only its SQL reader."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

import dpone.runtime.composition_transfer_preplan as module
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.composition_transfer_preplan_store import CompositionTransferPreplanStore
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import PreparedPostgresSourceBoundary
from tests.test_composition_transfer_preplan_store import originals
from tests.test_mssql_schema_preplan import _CatalogSink, _config


@pytest.fixture
def case(tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    attempt, manifest, write, physical, operation, expected, identity, projection = originals()
    expected_manifest = deepcopy(manifest)
    events = []
    config = _config({"postgres_source_authority_sha256": identity.authority_sha256})
    config.source_database = "source"
    config = module.bind_replay_safe_mssql_hook_graph(config)
    route = module.invocation_route_fingerprint(config, target_identity=physical.digest, source_identity=identity)
    operation = replace(
        operation,
        attempt=replace(operation.attempt, request=replace(operation.attempt.request, route_fingerprint=route)),
    )

    class Source:
        connector = object()
        selected = projection

        def fetch_schema_projection(self, supplied):
            assert "__dpone_mssql_prepared_postgres_source_boundary" not in supplied.options
            events.append("source_catalog")
            return self.selected

    source = Source()
    monkeypatch.setattr(module, "PostgresSource", Source)

    class Lease:
        def require_for(self, connector):
            assert connector is source.connector
            events.append("source_active")

    boundary = PreparedPostgresSourceBoundary(source.connector, Lease(), identity, projection)

    class Connector:
        connection = SimpleNamespace(autocommit=False)

        def execute_query(self, sql):
            assert sql.startswith("SET ")

        def rollback(self):
            events.append("target_rollback")

        def close(self):
            events.append("target_close")

    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    sink.connector = Connector()
    observed = SimpleNamespace(physical=physical, identity=identity, pins=b'{"marker":"checked"}')

    def target_factory(*args, **kwargs):
        assert kwargs == {"autocommit": False}
        events.append("target_open")
        return sink

    monkeypatch.setattr(module.ResolvedEndpointFactory, "create_sink", target_factory)

    def normalize(value, **kwargs):
        assert value == expected_manifest
        assert kwargs["connections"].source.credentials.username == "reader"
        assert kwargs["context"].environment == "test"
        events.append("normalize")
        return replace(config, options=dict(config.options))

    monkeypatch.setattr(module, "prepare_verified_transfer_config", normalize)

    class SourceVerifier:
        def verify_snapshot(self, *, connector, snapshot_lease, load_config):
            assert connector is source.connector and snapshot_lease is boundary.snapshot_lease
            events.append("source_identity")
            return observed.identity

    monkeypatch.setattr(module.PostgresSourceAuthorityVerifier, "from_connection", lambda *args: SourceVerifier())
    monkeypatch.setattr(module.MssqlSessionIdentity, "read", lambda *args: object())
    monkeypatch.setattr(module, "resolve_mssql_physical_target_identity", lambda *args, **kwargs: observed.physical)

    def require_target(connection):
        assert connection is sink.connector.connection
        if isinstance(observed.pins, Exception):
            raise observed.pins
        events.append("target_pins")
        return observed.pins

    credentials = CredentialsConfig(host="fixture", database="source", username="reader")
    source_target = ResolvedBindingConnection(credentials, {}, ResolvedConnectionDescriptor("postgres", {}))
    target = ResolvedBindingConnection(
        CredentialsConfig(host="fixture", database="DWH"), {}, ResolvedConnectionDescriptor("mssql", {})
    )
    journal = CompositionTransferPreplanStore(tmp_path)
    constructor = dict(
        verified_manifest=manifest,
        source_target=source_target,
        sink_target=target,
        state_target=target,
        parent_context=SimpleNamespace(environment="test"),
        attempt=attempt,
        write=write,
        plan_sha256=attempt.plan_sha256,
        journal=journal,
        require_target=require_target,
    )
    service = module.CompositionTransferPreplanService(**constructor)
    return SimpleNamespace(
        service=service,
        admission=MssqlTransactionAdmission(operation=operation),
        source=source,
        boundary=boundary,
        expected=expected,
        events=events,
        observed=observed,
        journal=journal,
        attempt=attempt,
        credentials=credentials,
        manifest=manifest,
        constructor=constructor,
    )


def invoke(case, digest=None):
    return case.service.verify_preplan(
        case.admission,
        case.source,
        case.boundary,
        case.expected.target_mutation_plan.digest if digest is None else digest,
    )


def test_independent_plan_uses_real_preplanner_and_preserves_source_boundary(case):
    reference = invoke(case)
    assert reference.preplan == case.expected
    assert case.journal.load(case.attempt, reference.document_sha256) == reference
    assert case.events[-2:] == ["target_rollback", "target_close"]
    assert case.events.index("source_identity") < case.events.index("target_open")
    assert case.events.count("source_catalog") >= 3
    case.boundary.require_active(case.source.connector)


@pytest.mark.parametrize("failure", ["target", "route", "mutation", "pins", "source", "projection"])
def test_mismatch_never_persists_and_closes_only_private_target(case, failure):
    submitted = None
    if failure == "target":
        case.observed.physical = replace(
            case.observed.physical, binding_id=UUID("12345678-1234-4234-8234-123456789013")
        )
    elif failure == "route":
        operation = case.admission.operation
        case.admission = MssqlTransactionAdmission(
            operation=replace(
                operation,
                attempt=replace(
                    operation.attempt, request=replace(operation.attempt.request, route_fingerprint=b"x" * 32)
                ),
            )
        )
    elif failure == "mutation":
        submitted = b"x" * 32
    elif failure == "pins":
        case.observed.pins = ValueError("denied")
    elif failure == "source":
        case.observed.identity = replace(case.observed.identity, relation_oid=5)
    else:
        case.source.selected = replace(case.source.selected, relation_schema=(("id", "integer"),))
    with pytest.raises(ValueError):
        invoke(case, submitted)
    with pytest.raises(FileNotFoundError):
        case.journal.load(case.attempt, b"x" * 32)
    if "target_open" in case.events:
        assert case.events[-2:] == ["target_rollback", "target_close"]
    case.boundary.require_active(case.source.connector)


def test_constructor_detaches_credentials_and_manifest(case):
    case.credentials.username = "changed"
    case.manifest["sink"]["table"]["name"] = "changed"
    invoke(case)


def test_second_capture_does_not_replay_or_overwrite_original(case):
    first = invoke(case)
    with pytest.raises(FileExistsError):
        invoke(case)
    assert case.journal.load(case.attempt, first.document_sha256) == first


def test_required_target_authority_callback_cannot_be_omitted():
    with pytest.raises(TypeError):
        module.CompositionTransferPreplanService()


def test_verified_environment_does_not_copy_resolver_clients(case):
    from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext

    class Resolver:
        def __deepcopy__(self, memo):
            raise AssertionError("resolver clients must not be copied")

    context = RuntimeConnectionContext("test", {}, {}, {}, Resolver())
    case.service = module.CompositionTransferPreplanService(**{**case.constructor, "parent_context": context})
    invoke(case)


@pytest.mark.parametrize("environment", [None, "", "   "])
def test_missing_verified_environment_is_rejected(case, environment):
    with pytest.raises(ValueError):
        module.CompositionTransferPreplanService(
            **{**case.constructor, "parent_context": SimpleNamespace(environment=environment)}
        )


@pytest.fixture
def retained_case(monkeypatch):
    from hashlib import sha256

    import dpone.runtime.sinks.mssql_target_catalog_fingerprint as catalog
    import dpone.runtime.state.mssql_target_identity as target_identity
    from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
    from dpone.contracts.composition_persistence import encode_activation_request
    from dpone.runtime.composition_transfer_preplan_store import decode_transfer_preplan
    from tests.composition_mssql_gate_helpers import SERVICE, occurrence
    from tests.test_composition_transfer_preplan_store import envelope
    from tests.test_mssql_generic_transaction_governance import _receipt

    attempt, document = envelope()
    reference = decode_transfer_preplan(document, sha256(document).digest(), attempt=attempt)
    _, _, write, physical, operation, _, _, _ = originals()
    binding = CompositionMssqlOperationBinding(
        attempt,
        operation,
        write,
        reference.mutation_plan_sha256,
        b"s" * 16,
        SERVICE,
        "Control",
        encode_activation_request(occurrence().request),
        "sha256:" + reference.document_sha256.hex(),
    )
    mutation = reference.preplan.target_mutation_plan
    receipt = replace(
        _receipt(operation),
        load_id=operation.attempt.request.load_id,
        mutation_plan_sha256=mutation.digest,
        target_before_sha256=mutation.expected_before_sha256,
        target_after_sha256=mutation.expected_after_sha256,
    )
    connector = object()
    observed = SimpleNamespace(
        physical=physical,
        snapshot=_CatalogSink([MssqlCatalogColumn("id", "bigint", True)]).get_target_catalog_snapshot(_config({})),
    )

    def resolve(actual_connector, **kwargs):
        assert actual_connector is connector
        return observed.physical

    monkeypatch.setattr(target_identity, "resolve_mssql_physical_target_identity", resolve)
    monkeypatch.setattr(module.MssqlSessionIdentity, "read", lambda actual: object())

    def read_catalog(owner, config):
        assert owner.connector is connector
        assert (config.target_database, config.target_schema, config.target_table) == (
            write.database,
            write.schema,
            write.relation,
        )
        return observed.snapshot

    monkeypatch.setattr(catalog, "read_schema_catalog_snapshot", read_catalog)
    return SimpleNamespace(
        reference=reference,
        binding=binding,
        receipt=receipt,
        connector=connector,
        observed=observed,
        payload=SimpleNamespace(source_provenance_sha256=reference.body["source_provenance_sha256"]),
    )


def verify_retained(value):
    module.verify_retained_transfer_commit(
        value.reference, binding=value.binding, receipt=value.receipt, payload=value.payload, connector=value.connector
    )


def test_retained_commit_checks_actual_registry_and_catalog_assertions(retained_case):
    verify_retained(retained_case)


@pytest.mark.parametrize(
    "damage", ["legacy", "pin", "attempt", "before", "after", "provenance", "registry", "catalog", "operation", "write"]
)
def test_retained_commit_rejects_foreign_or_drifted_originals(retained_case, damage):
    value = retained_case
    if damage == "legacy":
        value.binding = replace(value.binding, preplan_document_sha256=None)
    elif damage == "pin":
        value.binding = replace(value.binding, preplan_document_sha256="sha256:" + "f" * 64)
    elif damage == "attempt":
        value.binding = replace(
            value.binding, attempt=replace(value.binding.attempt, try_number=value.binding.attempt.try_number + 1)
        )
    elif damage in {"before", "after"}:
        value.receipt = replace(value.receipt, **{"target_" + damage + "_sha256": b"x" * 32})
    elif damage == "provenance":
        value.payload.source_provenance_sha256 = "f" * 64
    elif damage == "registry":
        value.observed.physical = replace(
            value.observed.physical, binding_id=UUID("12345678-1234-4234-8234-123456789013")
        )
    elif damage == "catalog":
        value.observed.snapshot = replace(value.observed.snapshot, database_collation="SQL_Latin1_General_CP1_CI_AS")
    else:
        from dpone.contracts.strict_json import canonical_json_bytes
        from dpone.runtime.composition_transfer_preplan_store import RetainedTransferPreplanReference

        body = value.reference.body
        if damage == "operation":
            body["operation_original"]["attempt"]["request"]["load_id"] = "other"
        else:
            body["write"]["workflow_id"] = "other"
        value.reference = RetainedTransferPreplanReference(canonical_json_bytes(body))
        value.binding = replace(
            value.binding, preplan_document_sha256="sha256:" + value.reference.document_sha256.hex()
        )
    with pytest.raises((ValueError, RuntimeError)):
        verify_retained(value)


def test_registry_identity_survives_object_creation(retained_case):
    from dpone.contracts.strict_json import canonical_json_bytes
    from dpone.runtime.composition_transfer_preplan_store import RetainedTransferPreplanReference

    value = retained_case
    body = value.reference.body
    body["target_identity"]["object_id"] = None
    value.reference = RetainedTransferPreplanReference(canonical_json_bytes(body))
    value.binding = replace(value.binding, preplan_document_sha256="sha256:" + value.reference.document_sha256.hex())
    # Real registry assertion hashes binding UUID/database incarnation, not the
    # ephemeral object_id; final catalog expectations are verified separately.
    assert value.observed.physical.object_id is not None
    verify_retained(value)
