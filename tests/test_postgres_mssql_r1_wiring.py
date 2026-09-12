from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.postgres_mssql_correctness_profile import (
    CATALOG_SCHEMA,
    MappingPostgresMssqlCorrectnessCatalog,
    PostgresMssqlCorrectnessCatalogError,
)
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.runtime_bootstrap import build_default_runtime_hydrator
from dpone.app.settings import Settings
from dpone.commands.plan_cmd import _render_md, _render_text
from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.configuration_errors import ETLConfigurationError
from dpone.contracts.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessProfile,
    PostgresMssqlCorrectnessRouteRequest,
    SourceMode,
    decide_postgres_mssql_correctness_profile,
)
from dpone.dag.config_models import ETLProcessConfig
from dpone.readiness.managed import ConnectorScaffoldService, ExecutionPlanService
from dpone.readiness.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessProfileResolutionError,
    PostgresMssqlCorrectnessProfileResolver,
)
from dpone.readiness.postgres_mssql_correctness_route import (
    PostgresMssqlCorrectnessRouteResolver,
    postgres_mssql_correctness_projection,
    postgres_mssql_correctness_request,
    postgres_mssql_correctness_runtime_request,
)
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.etl.extracted_payload_load import ExtractedPayloadLoadService
from dpone.runtime.etl.processor_runtime import ProcessorRuntimeServices
from dpone.runtime.postgres_mssql_r1_execution import (
    PostgresMssqlR1ExecutionError,
    bind_postgres_mssql_r1_execution,
)
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import (
    MSSQLFullRefreshStrategy,
    MSSQLIncrementMergeStrategy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_staging_consumer import MssqlStagingConsumer
from dpone.runtime.streaming_rows import StreamingRowsArtifact


def test_forged_exact_source_schema_runtime_rejects_before_endpoint_access() -> None:
    try:
        from dpone.runtime.postgres_mssql_source_schema_runtime import (
            PostgresMssqlSourceSchemaRuntimeV1,
        )
    except (ImportError, ModuleNotFoundError):
        pytest.fail("approved source-schema runtime composition is missing", pytrace=False)
    try:
        from dpone.runtime.bootstrap_postgres_source_authority import (
            bind_postgres_mssql_source_schema_runtime,
        )
    except (ImportError, ModuleNotFoundError):
        pytest.fail("approved source-schema runtime composition is missing", pytrace=False)

    endpoint_events: list[str] = []

    class SourceSpy:
        def __getattribute__(self, name: str):
            if name != "__class__":
                endpoint_events.append(name)
                raise AssertionError("endpoint touched before runtime validation")
            return object.__getattribute__(self, name)

    forged = object.__new__(PostgresMssqlSourceSchemaRuntimeV1)
    error = None
    try:
        bind_postgres_mssql_source_schema_runtime(
            source_obj=SourceSpy(),
            runtime=forged,
        )
    except RuntimeConfigurationError as exc:
        error = exc
    except Exception:
        raise AssertionError("forged exact runtime did not fail through the stable runtime contract") from None
    assert error is not None
    assert error.__cause__ is error.__context__ is None
    assert endpoint_events == []


def _profile(**overrides: object) -> PostgresMssqlCorrectnessProfile:
    values: dict[str, object] = {
        "profile_id": "postgres-mssql-r1",
        "source_connection_ref": "postgres_orders",
        "sink_connection_ref": "mssql_dwh",
        "allowed_source_modes": (SourceMode.BATCH_FULL_REFRESH, SourceMode.XMIN_CURRENT_STATE),
        "source_major": 16,
        "target_major": 2022,
        "topology": "standalone_same_database",
        "object_profile": "ordinary_disk_rowstore",
        "key_types": ("int2", "int4", "int8", "uuid"),
        "receipt_contract": "mssql_effect_receipt_v2",
        "hash_policy": "postgres_mssql_row_hash_v1",
        "writer_fence": "mssql_target_head_v2",
        "session_count": 1,
        "transaction_scope": "local_database",
        "delayed_durability_disabled": True,
        "max_descendant_proof_receipts": 100_000,
        "target_binding_uuid": "3316d0bd-3d61-4a25-a14e-bf21fe038e37",
        "target_contract_revision": 7,
        "quality_policy": "postgres_mssql_r1_bounded_v1",
        "certification_ref": "postgres-mssql-r1-vendor-live",
        "implementation_status": "implemented",
        "certification_status": "vendor_pass",
        "activation_status": "explicit_opt_in",
    }
    values.update(overrides)
    return PostgresMssqlCorrectnessProfile(**values)


def _request(**overrides: object) -> PostgresMssqlCorrectnessRouteRequest:
    values: dict[str, object] = {
        "source_connection_ref": "postgres_orders",
        "sink_connection_ref": "mssql_dwh",
        "source_mode": SourceMode.BATCH_FULL_REFRESH,
        "target_schema": "dbo",
        "target_table": "orders",
        "business_key_types": ("int8",),
    }
    values.update(overrides)
    return PostgresMssqlCorrectnessRouteRequest(**values)


class _Selector:
    def __init__(self, profile_id: str | None) -> None:
        self.profile_id = profile_id
        self.requests: list[PostgresMssqlCorrectnessRouteRequest] = []

    def select_profile_id(self, request: PostgresMssqlCorrectnessRouteRequest) -> str | None:
        self.requests.append(request)
        return self.profile_id


class _Profiles:
    def __init__(self, profile: PostgresMssqlCorrectnessProfile) -> None:
        self.profile = profile
        self.loads = 0

    def load(self, profile_id: str) -> PostgresMssqlCorrectnessProfile:
        self.loads += 1
        assert profile_id == self.profile.profile_id
        return self.profile


class _Evidence:
    def __init__(self) -> None:
        self.binds = 0

    def bind_current_evidence(
        self,
        profile: PostgresMssqlCorrectnessProfile,
    ) -> PostgresMssqlCorrectnessProfile:
        self.binds += 1
        return profile


def _route(
    selector: _Selector,
    profiles: _Profiles,
    evidence: _Evidence,
) -> PostgresMssqlCorrectnessRouteResolver:
    return PostgresMssqlCorrectnessRouteResolver(
        selector=selector,
        resolver=PostgresMssqlCorrectnessProfileResolver(profiles, evidence),
    )


def test_unregistered_route_remains_compatibility_without_profile_or_evidence_reads() -> None:
    selector = _Selector(None)
    profiles = _Profiles(_profile())
    evidence = _Evidence()

    decision = _route(selector, profiles, evidence).resolve(_request())

    assert decision is None
    assert profiles.loads == 0
    assert evidence.binds == 0


def test_registered_route_resolves_one_profile_snapshot_and_binds_exact_coordinates() -> None:
    selector = _Selector("postgres-mssql-r1")
    profiles = _Profiles(_profile())
    evidence = _Evidence()

    decision = _route(selector, profiles, evidence).require_activatable(_request())

    assert decision is not None
    assert decision.profile_id == "postgres-mssql-r1"
    assert decision.source_mode is SourceMode.BATCH_FULL_REFRESH
    assert profiles.loads == 1
    assert evidence.binds == 1


def test_metadata_and_runtime_projection_share_exact_selector_coordinates() -> None:
    spec = _spec()

    runtime_request = postgres_mssql_correctness_runtime_request(
        source_type="postgres",
        sink_type="mssql",
        strategy="full_refresh",
        load_config=spec.config.load_config,
        raw_config=spec.raw_config,
    )

    assert runtime_request == postgres_mssql_correctness_request(spec)


def test_runtime_binding_preserves_immutable_r1_decision_for_composition_root() -> None:
    spec = _spec()
    request = postgres_mssql_correctness_request(spec)
    assert request is not None
    decision = decide_postgres_mssql_correctness_profile(
        request.requirements(),
        _profile(),
    )
    bindings = SimpleNamespace(
        source_obj=object(),
        sink_obj=object(),
        etl_logger=object(),
        run_state_storage=None,
        xmin_handoff_state_storage=None,
        partition_checkpoint_store=None,
        load_identity_service=None,
        postgres_mssql_correctness_activation=decision,
        credential_resolution_receipts=(),
    )

    config = ETLProcessConfig(name="orders", load_config=spec.config.load_config)
    config.apply_runtime_bindings(bindings)

    assert config.postgres_mssql_correctness_activation is decision


def test_runtime_hydrator_requires_selected_profile_before_endpoint_build() -> None:
    spec = _spec()
    expected = object()

    class Resolver:
        def require_runtime_activatable(self, **kwargs):
            request = postgres_mssql_correctness_runtime_request(**kwargs)
            assert request == postgres_mssql_correctness_request(spec)
            return expected

    connection = lambda kind: SimpleNamespace(  # noqa: E731 - compact immutable test fixture.
        descriptor=SimpleNamespace(connection_type=kind)
    )
    decision = DefaultRuntimeHydrator(
        postgres_mssql_correctness_route_resolver=Resolver()
    )._resolve_postgres_mssql_correctness(
        config=spec.raw_config,
        load_config=spec.config.load_config,
        connections=SimpleNamespace(source=connection("postgres"), sink=connection("mssql")),
        source_cfg=spec.raw_config["source"],
        sink_cfg=spec.raw_config["sink"],
    )

    assert decision is expected


def test_runtime_hydrator_lazily_resolves_platform_catalog_before_endpoints() -> None:
    spec = _spec()
    calls: list[str] = []

    class Resolver:
        def require_runtime_activatable(self, **_kwargs):
            calls.append("resolve")
            return None

    hydrator = DefaultRuntimeHydrator(
        postgres_mssql_correctness_route_resolver_provider=lambda: Resolver(),
    )
    connection = lambda kind: SimpleNamespace(  # noqa: E731 - compact immutable fixture.
        descriptor=SimpleNamespace(connection_type=kind)
    )

    assert (
        hydrator._resolve_postgres_mssql_correctness(
            config=spec.raw_config,
            load_config=spec.config.load_config,
            connections=SimpleNamespace(source=connection("postgres"), sink=connection("mssql")),
            source_cfg=spec.raw_config["source"],
            sink_cfg=spec.raw_config["sink"],
        )
        is None
    )
    assert calls == ["resolve"]


def test_default_runtime_hydrator_has_lazy_r1_catalog_composition() -> None:
    hydrator = build_default_runtime_hydrator()

    assert hydrator._postgres_mssql_correctness_route_resolver_provider is not None


def test_r1_runtime_replaces_legacy_admission_in_processor_services() -> None:
    calls: list[str] = []

    class R1Runtime:
        def prepare_admission(self, load_config, **_kwargs):
            calls.append("prepare")
            return bind_postgres_mssql_r1_execution(load_config, self)

        def replay_result(self, _load_config):
            calls.append("replay")
            return None

    services = ProcessorRuntimeServices(
        source=object(),
        sink=object(),
        source_state_service=object(),
        payload_load_service=object(),
        postgres_mssql_correctness_runtime=R1Runtime(),
    )
    load_config = _spec().config.load_config

    assert services.mssql_transaction_admission_service is None
    admitted_config = services.prepare_admission(
        load_config,
        run_context=None,
        load_record=None,
        dag_id=None,
    )
    assert admitted_config is not load_config
    assert services.replay_result(admitted_config) is None
    assert calls == ["prepare", "replay"]


def test_r1_runtime_must_bind_its_exact_context_before_source_io() -> None:
    class BrokenRuntime:
        def prepare_admission(self, load_config, **_kwargs):
            return load_config

    services = ProcessorRuntimeServices(
        source=object(),
        sink=object(),
        source_state_service=object(),
        payload_load_service=object(),
        postgres_mssql_correctness_runtime=BrokenRuntime(),
    )

    with pytest.raises(PostgresMssqlR1ExecutionError, match="context_missing"):
        services.prepare_admission(
            _spec().config.load_config,
            run_context=None,
            load_record=None,
            dag_id=None,
        )


def test_r1_runtime_context_survives_copy_and_is_rechecked_before_load() -> None:
    runtime = object()
    original = _spec().config.load_config
    prepared = bind_postgres_mssql_r1_execution(original, runtime)

    assert prepared is not original
    assert original.options.get("__dpone_postgres_mssql_r1_execution") is None
    ProcessorRuntimeServices(
        source=object(),
        sink=object(),
        source_state_service=object(),
        payload_load_service=object(),
        postgres_mssql_correctness_runtime=runtime,
    ).require_execution_context(prepared)


def test_r1_batch_dispatch_never_constructs_legacy_staging_consumer() -> None:
    calls: list[str] = []

    class Runtime:
        def load_batch(self, *, strategy, load_config, payload):
            calls.append("batch")
            return "v2-batch"

    strategy = object.__new__(MSSQLFullRefreshStrategy)
    strategy.staging_consumer_factory = lambda _strategy: pytest.fail("legacy staging consumer constructed")
    payload = SimpleNamespace(postgres_mssql_r1_execution=Runtime())

    assert strategy.load(object(), payload) == "v2-batch"
    assert calls == ["batch"]


def test_r1_xmin_dispatch_never_constructs_legacy_snapshot_finalizer() -> None:
    calls: list[str] = []

    class Runtime:
        def load_xmin(self, *, strategy, load_config, payload):
            calls.append("xmin")
            return "v2-xmin"

    strategy = object.__new__(MSSQLIncrementMergeStrategy)
    payload = SimpleNamespace(postgres_mssql_r1_execution=Runtime())

    assert strategy.load(object(), payload) == "v2-xmin"
    assert calls == ["xmin"]


def test_legacy_staging_consumer_rejects_lost_r1_dispatch() -> None:
    payload = SimpleNamespace(postgres_mssql_r1_execution=object())

    with pytest.raises(PostgresMssqlR1ExecutionError, match="legacy_dispatch_forbidden"):
        MssqlStagingConsumer(SimpleNamespace()).consume(object(), payload, object())


def test_r1_execution_identity_is_carried_into_load_payload() -> None:
    execution = object()
    captured: list[Any] = []

    class Sink:
        @staticmethod
        def native_lineage_projection_capability() -> str:
            return "mssql_native_lineage_v1"

    class Loader:
        @staticmethod
        def load_single_payload(_config, payload, *_args, **_kwargs):
            captured.append(payload)
            return SimpleNamespace(reconciliation_metrics=None)

    config = bind_postgres_mssql_r1_execution(
        LoadConfig(
            source_conn_id="postgres_orders",
            target_conn_id="mssql_dwh",
            source_schema="public",
            source_table="orders",
            target_schema="dbo",
            target_table="orders",
            load_strategy=LoadStrategy.FULL_REFRESH,
            options={"source_type": "postgres", "sink_type": "mssql"},
        ),
        execution,
    )
    service = ExtractedPayloadLoadService(
        sink=Sink(),
        logger=SimpleNamespace(log_etl_progress=lambda *_args: None),
        load_identity_service=object(),
        payload_load_service=Loader(),
    )
    artifact = StreamingRowsArtifact(iter(({"order_id": 1},)))

    service.load_extracted_payload(
        load_config=config,
        extract_result=SimpleNamespace(
            artifact=artifact,
            schema=(("order_id", "Int64"),),
            relation_schema=None,
            relation_metadata=None,
            relation_dialect="postgres",
            target_projection=None,
            extraction_lifecycle=None,
        ),
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
        reconciliation_service=SimpleNamespace(run_if_enabled=lambda *_args, **_kwargs: None),
    )

    assert len(captured) == 1
    assert captured[0].postgres_mssql_r1_execution is execution


def test_r1_runtime_rejects_concurrent_legacy_admission_service() -> None:
    with pytest.raises(RuntimeError, match="postgres_mssql_r1.legacy_admission_conflict"):
        ProcessorRuntimeServices(
            source=object(),
            sink=object(),
            source_state_service=object(),
            payload_load_service=object(),
            mssql_transaction_admission_service=object(),
            postgres_mssql_correctness_runtime=object(),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_connection_ref", "other_postgres"),
        ("sink_connection_ref", "other_mssql"),
    ],
)
def test_selector_cannot_rebind_profile_to_different_connections(field: str, value: object) -> None:
    selector = _Selector("postgres-mssql-r1")
    profiles = _Profiles(replace(_profile(), **{field: value}))

    with pytest.raises(PostgresMssqlCorrectnessProfileResolutionError) as error:
        _route(selector, profiles, _Evidence()).resolve(_request())

    assert error.value.code == "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"
    assert profiles.loads == 1


def test_selected_but_unverified_route_is_not_activatable() -> None:
    selector = _Selector("postgres-mssql-r1")
    profile = replace(_profile(), certification_status="unverified", activation_status="blocked")

    with pytest.raises(PostgresMssqlCorrectnessProfileResolutionError) as error:
        _route(selector, _Profiles(profile), _Evidence()).require_activatable(_request())

    assert error.value.code == "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"
    assert isinstance(error.value, ETLConfigurationError)


def _catalog_payload() -> dict[str, Any]:
    profile = _profile()
    fields = {
        name: getattr(profile, name)
        for name in profile.__dataclass_fields__
        if name not in {"profile_id", "implementation_status", "certification_status", "activation_status"}
    }
    fields["allowed_source_modes"] = [mode.value for mode in profile.allowed_source_modes]
    fields["key_types"] = list(profile.key_types)
    return {
        "schema": CATALOG_SCHEMA,
        "profiles": {profile.profile_id: fields},
        "registrations": [
            {
                "source_connection_ref": "postgres_orders",
                "sink_connection_ref": "mssql_dwh",
                "source_mode": "batch_full_refresh",
                "target_schema": "dbo",
                "target_table": "orders",
                "profile_id": profile.profile_id,
            }
        ],
    }


def test_mapping_catalog_selects_only_exact_registration_and_never_trusts_status_claims() -> None:
    payload = _catalog_payload()
    raw_profile = payload["profiles"]["postgres-mssql-r1"]
    raw_profile["certification_status"] = "vendor_pass"
    catalog = MappingPostgresMssqlCorrectnessCatalog(payload)

    selected = catalog.select_profile_id(_request())
    profile = catalog.load("postgres-mssql-r1")

    assert selected == "postgres-mssql-r1"
    assert catalog.select_profile_id(_request(target_table="other")) is None
    assert profile.implementation_status == "absent"
    assert profile.certification_status == "unverified"
    assert profile.activation_status == "blocked"


def test_mapping_catalog_rejects_ambiguous_exact_registration() -> None:
    payload = _catalog_payload()
    payload["registrations"] = [*payload["registrations"], *payload["registrations"]]

    with pytest.raises(PostgresMssqlCorrectnessCatalogError) as error:
        MappingPostgresMssqlCorrectnessCatalog(payload)

    assert error.value.code == "DPONE_POSTGRES_MSSQL_PROFILE_REGISTRATION_AMBIGUOUS"


def _spec(*, strategy: str = "full_refresh", incremental_strategy: str | None = None) -> SimpleNamespace:
    options = {"source_type": "postgres", "sink_type": "mssql"}
    if incremental_strategy is not None:
        options["incremental_strategy"] = incremental_strategy
    load_config = SimpleNamespace(
        source_conn_id="postgres_orders",
        target_conn_id="mssql_dwh",
        target_schema="dbo",
        target_table="orders",
        unique_key="order_id",
        options=options,
        load_strategy=SimpleNamespace(value=strategy),
    )
    return SimpleNamespace(
        raw_config={
            "source": {
                "type": "postgres",
                "options": {"columns": {"order_id": "bigint", "amount": "numeric(18,2)"}},
            },
            "sink": {"type": "mssql"},
        },
        config=SimpleNamespace(load_config=load_config),
    )


def test_process_projection_uses_semantics_and_declared_key_without_physical_identity() -> None:
    request = postgres_mssql_correctness_request(_spec())

    assert request == _request()
    projection = postgres_mssql_correctness_projection(
        _route(_Selector("postgres-mssql-r1"), _Profiles(_profile()), _Evidence()).resolve(request)
    )
    assert projection["selected"] is True
    assert projection["capability_id"] == "postgres_mssql_target_uow_v2"
    assert projection["source_mode"] == "batch_full_refresh"
    assert "target_binding_uuid" not in projection
    assert "certification_ref" not in projection


def test_process_projection_selects_xmin_before_strategy_and_marks_unknown_key_fail_closed() -> None:
    spec = _spec(strategy="incremental_merge", incremental_strategy="xmin")
    spec.raw_config["source"]["options"]["columns"] = {"other": "uuid"}

    request = postgres_mssql_correctness_request(spec)

    assert request is not None
    assert request.source_mode is SourceMode.XMIN_CURRENT_STATE
    assert request.business_key_types == ("unknown",)
    decision = _route(_Selector("postgres-mssql-r1"), _Profiles(_profile()), _Evidence()).resolve(request)
    assert decision is not None
    assert decision.blockers == ("DPONE_POSTGRES_MSSQL_UNSUPPORTED_KEY",)


class _PlanningRouteResolver:
    def resolve(self, request: PostgresMssqlCorrectnessRouteRequest):
        profile = replace(
            _profile(),
            source_connection_ref=request.source_connection_ref,
            sink_connection_ref=request.sink_connection_ref,
        )
        return decide_postgres_mssql_correctness_profile(request.requirements(), profile)


def test_execution_plan_projects_the_same_injected_r1_decision(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.batch.yaml"
    ConnectorScaffoldService().generate_init_bundle(
        output_path=manifest,
        source_type="postgres",
        sink_type="mssql",
        source_connection="postgres_orders",
        sink_connection="mssql_dwh",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        strategy="full_refresh",
        unique_key="order_id",
    )
    payload = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    payload["defaults"]["source"]["options"]["columns"] = {"order_id": "bigint"}
    manifest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    plan = ExecutionPlanService(postgres_mssql_correctness=_PlanningRouteResolver()).plan_manifest(
        manifest, selector="public.orders"
    )

    projection = plan["postgres_mssql_correctness"]
    assert projection["selected"] is True
    assert projection["source_mode"] == "batch_full_refresh"
    assert projection["certification_status"] == "vendor_pass"
    assert projection["mutates"] is False

    assert "postgres_mssql_target_uow_v2" in _render_text(plan)
    assert "## PostgreSQL to MSSQL correctness" in _render_md(plan)


def test_app_context_catalog_can_select_but_never_locally_promote_r1(tmp_path: Path) -> None:
    catalog_path = tmp_path / "postgres-mssql-correctness.yml"
    catalog_path.write_text(yaml.safe_dump(_catalog_payload(), sort_keys=False), encoding="utf-8")
    settings = Settings(
        repo_root=tmp_path,
        project_dir=tmp_path,
        manifest_dir=tmp_path,
        sources_registry_paths=(),
        postgres_mssql_correctness_catalog_path=catalog_path,
    )
    context = AppContext(
        settings=settings,
        logger=SimpleNamespace(),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )

    resolver = context.build_postgres_mssql_correctness_route_resolver()
    assert resolver is not None
    decision = resolver.resolve(_request())

    assert decision is not None
    assert decision.implementation_status == "absent"
    assert decision.certification_status == "unverified"
    assert decision.activation_status == "blocked"


def test_source_schema_composition_rejects_wrong_runtime_before_endpoint_construction() -> None:
    try:
        from dpone.runtime.bootstrap_postgres_source_authority import bind_postgres_mssql_source_schema_runtime
    except ImportError:
        pytest.fail("approved source-schema composition helper is missing", pytrace=False)

    events: list[str] = []

    class Source:
        def bind_postgres_mssql_source_schema_runtime(self, runtime) -> None:
            events.append("bind")
            self.runtime = runtime

    with pytest.raises(RuntimeConfigurationError) as raised:
        bind_postgres_mssql_source_schema_runtime(source_obj=Source(), runtime=object())
    assert raised.value.code == "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_RUNTIME_REQUIRED"
    assert events == []


def test_source_schema_factory_cannot_substitute_another_valid_verifier_before_endpoints() -> None:
    try:
        from dpone.runtime.bootstrap_postgres_source_authority import build_postgres_mssql_source_schema_runtime
        from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
        from tests.test_postgres_mssql_r1_source_schema_runtime import _policy, _source_authority
        from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules
    except ImportError:
        pytest.fail("approved source-schema composition is missing", pytrace=False)

    modules = _feature_modules()
    selected_verifier = PostgresSourceAuthorityVerifier(_source_authority())
    substituted_verifier = PostgresSourceAuthorityVerifier(_source_authority())
    assert substituted_verifier is not selected_verifier
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    runtime = modules["runtime"].PostgresMssqlSourceSchemaRuntimeV1(
        verifier=substituted_verifier,
        snapshot_scope_issuer=modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1(),
        schema_authority_issuer=issuer,
        projection_adapter=modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(
            fetched_schema_factory=modules["boundary"].build_r1_postgres_fetched_schema
        ),
    )

    class SubstitutingFactory:
        def build(self, **kwargs):
            assert kwargs["verifier"] is selected_verifier
            return runtime

    endpoint_events: list[str] = []
    error = None
    try:
        result = build_postgres_mssql_source_schema_runtime(
            object(),
            SubstitutingFactory(),
            object(),
            object(),
            selected_verifier,
        )
    except RuntimeConfigurationError as exc:
        error = exc
    else:
        endpoint_events.append("endpoint-construction-would-start")
        assert result is runtime

    assert getattr(error, "code", "") == "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_RUNTIME_REQUIRED"
    assert error is not None
    assert error.__cause__ is error.__context__ is None
    assert endpoint_events == []


def test_source_retains_admitted_runtime_and_rejects_conflicting_rebind() -> None:
    try:
        from dpone.runtime.postgres_mssql_source_schema_runtime import PostgresMssqlSourceSchemaRuntimeV1
    except ImportError:
        pytest.fail("approved source-schema runtime is missing", pytrace=False)

    source = object.__new__(__import__("dpone.runtime.sources.postgres", fromlist=["PostgresSource"]).PostgresSource)
    runtime = object.__new__(PostgresMssqlSourceSchemaRuntimeV1)
    source._postgres_mssql_source_schema_runtime = None
    source.bind_postgres_mssql_source_schema_runtime(runtime)
    source.bind_postgres_mssql_source_schema_runtime(runtime)
    with pytest.raises(ValueError, match="already_bound"):
        source.bind_postgres_mssql_source_schema_runtime(object.__new__(PostgresMssqlSourceSchemaRuntimeV1))
    assert source._postgres_mssql_source_schema_runtime is runtime


_V12_BOUNDARY_MODULE = "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary"
_V12_CURRENT_MODULE_NAMESPACE = "<current-module-namespace>"
_V12_MODULE_OBJECT_PREFIX = "<module-object>:"
_V12_MODULE_MAPPING_PREFIX = "<module-mapping>:"
_V12_MODULE_REGISTRY_PREFIX = "<module-registry>:"
_V12_BOUND_SAFE_ACCESSOR_PREFIX = "<bound-safe-accessor>:"
_V12_BOUND_NAMESPACE_MUTATOR_PREFIX = "<bound-namespace-mutator>:"
_V12_DYNAMIC_MODULE_BINDING = "<dynamic-module-binding>"


def _v12_parse(path: Path):
    import ast

    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path)) if path.is_file() else ast.parse("")


def _v12_module_name(path: Path) -> str:
    without_suffix = path.with_suffix("")
    parts = without_suffix.parts
    return ".".join(parts[1:] if parts and parts[0] == "src" else parts)


def _v12_import_from_module(node: Any, current_module: str) -> str:
    """Resolve one absolute or package-relative ImportFrom module."""

    if node.level == 0:
        return node.module or "<missing-absolute-module>"
    current_parts = current_module.split(".")
    package_parts = current_parts[:-1]
    retained = len(package_parts) - (node.level - 1)
    if retained < 1:
        return "<relative-import-outside-package>"
    imported_parts = (node.module or "").split(".") if node.module else []
    return ".".join([*package_parts[:retained], *imported_parts])


def _v12_target_names(target: Any) -> tuple[str, ...]:
    import ast

    if isinstance(target, ast.Name):
        return (target.id,)
    if isinstance(target, ast.Starred):
        return _v12_target_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        return tuple(name for item in target.elts for name in _v12_target_names(item))
    return ()


def _v12_same_scope_nodes(statements: list[Any]) -> tuple[Any, ...]:
    """Walk lexical suites without entering nested callable/class scopes."""

    import ast

    observed: list[Any] = []

    def visit(node: Any) -> None:
        observed.append(node)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            return
        for child in ast.iter_child_nodes(node):
            visit(child)

    for statement in statements:
        visit(statement)
    return tuple(observed)


def _v12_top_level_bindings(tree: Any) -> tuple[tuple[str, str], ...]:
    import ast

    bindings: list[tuple[str, str]] = []
    for node in _v12_same_scope_nodes(tree.body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            bindings.append((node.name, "function"))
        elif isinstance(node, ast.ClassDef):
            bindings.append((node.name, "class"))
        elif isinstance(node, ast.ImportFrom):
            bindings.extend((alias.asname or alias.name, "import") for alias in node.names if alias.name != "*")
        elif isinstance(node, ast.Import):
            bindings.extend((alias.asname or alias.name.split(".", 1)[0], "import") for alias in node.names)
        elif isinstance(node, ast.Assign):
            bindings.extend((name, "assignment") for target in node.targets for name in _v12_target_names(target))
        elif isinstance(node, ast.AnnAssign):
            bindings.extend((name, "assignment") for name in _v12_target_names(node.target))
        elif type(node).__name__ == "TypeAlias":
            bindings.extend((name, "type_alias") for name in _v12_target_names(node.name))
    return tuple(bindings)


def _v12_top_level_provenance(tree: Any, module_name: str) -> tuple[tuple[str, str, str], ...]:
    """Resolve every module-scope binding with generic fail-closed provenance."""

    import ast

    kinds: dict[str, set[str]] = {}
    origins: dict[str, set[str]] = {}
    constant_strings: dict[str, set[str]] = {}
    rules: list[tuple[Any, Any, bool]] = []
    mutation_targets: list[tuple[Any, Any]] = []
    calls: list[Any] = []

    def register_names(target: Any, kind: str) -> None:
        for name in _v12_target_names(target):
            kinds.setdefault(name, set()).add(kind)

    def register_rule(target: Any, value: Any, *, pairwise: bool, kind: str) -> None:
        register_names(target, kind)
        rules.append((target, value, pairwise))

    def visit_expression(expression: Any) -> None:
        if expression is None:
            return
        if isinstance(expression, ast.Lambda):
            for default in (*expression.args.defaults, *expression.args.kw_defaults):
                visit_expression(default)
            return
        if isinstance(expression, ast.NamedExpr):
            register_rule(expression.target, expression.value, pairwise=True, kind="named_expression")
            visit_expression(expression.value)
            return
        if isinstance(expression, ast.Call):
            calls.append(expression)
        if isinstance(expression, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)):
            produced = (expression.key, expression.value) if isinstance(expression, ast.DictComp) else (expression.elt,)
            for item in produced:
                visit_expression(item)
            for generator in expression.generators:
                visit_expression(generator.iter)
                for condition in generator.ifs:
                    visit_expression(condition)
            return
        for child in ast.iter_child_nodes(expression):
            visit_expression(child)

    def pattern_target(pattern: Any) -> Any:
        names: list[str] = []
        for node in ast.walk(pattern):
            if isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name is not None:
                names.append(node.name)
            elif isinstance(node, ast.MatchMapping) and node.rest is not None:
                names.append(node.rest)
        return ast.Tuple(
            elts=[ast.Name(id=name, ctx=ast.Store()) for name in names],
            ctx=ast.Store(),
        )

    def visit_statements(statements: list[Any]) -> None:
        for statement in statements:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                kind = "class" if isinstance(statement, ast.ClassDef) else "function"
                kinds.setdefault(statement.name, set()).add(kind)
                origins.setdefault(statement.name, set()).add(f"{module_name}.{statement.name}")
                headers: list[Any] = [*statement.decorator_list]
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    headers.extend(statement.args.defaults)
                    headers.extend(default for default in statement.args.kw_defaults if default is not None)
                    headers.extend(
                        annotation
                        for argument in (
                            *statement.args.posonlyargs,
                            *statement.args.args,
                            *statement.args.kwonlyargs,
                        )
                        if (annotation := argument.annotation) is not None
                    )
                    headers.append(statement.returns)
                else:
                    headers.extend(statement.bases)
                    headers.extend(keyword.value for keyword in statement.keywords)
                headers.extend(getattr(statement, "type_params", ()))
                for header in headers:
                    visit_expression(header)
                if statement.decorator_list:
                    register_rule(
                        ast.Name(id=statement.name, ctx=ast.Store()),
                        ast.Tuple(elts=list(statement.decorator_list), ctx=ast.Load()),
                        pairwise=False,
                        kind=kind,
                    )
                continue
            if isinstance(statement, ast.ImportFrom):
                imported_module = _v12_import_from_module(statement, module_name)
                for imported in statement.names:
                    if imported.name == "*":
                        continue
                    local = imported.asname or imported.name
                    kinds.setdefault(local, set()).add("import")
                    origins.setdefault(local, set()).add(f"{imported_module}.{imported.name}")
                continue
            if isinstance(statement, ast.Import):
                for imported in statement.names:
                    local = imported.asname or imported.name.split(".", 1)[0]
                    resolved = imported.name if imported.asname is not None else local
                    kinds.setdefault(local, set()).add("import")
                    origins.setdefault(local, set()).update((resolved, f"{_V12_MODULE_OBJECT_PREFIX}{resolved}"))
                continue
            if isinstance(statement, ast.Assign):
                for target in statement.targets:
                    register_rule(target, statement.value, pairwise=True, kind="assignment")
                    if isinstance(target, (ast.Attribute, ast.Subscript)):
                        mutation_targets.append((target, statement.value))
                    visit_expression(target)
                visit_expression(statement.value)
                continue
            if isinstance(statement, ast.AnnAssign):
                if statement.value is not None:
                    register_rule(statement.target, statement.value, pairwise=True, kind="assignment")
                    if isinstance(statement.target, (ast.Attribute, ast.Subscript)):
                        mutation_targets.append((statement.target, statement.value))
                    visit_expression(statement.target)
                    visit_expression(statement.value)
                else:
                    register_names(statement.target, "assignment")
                visit_expression(statement.annotation)
                continue
            if type(statement).__name__ == "TypeAlias":
                register_rule(statement.name, statement.value, pairwise=True, kind="type_alias")
                visit_expression(statement.value)
                continue
            if isinstance(statement, ast.AugAssign):
                register_rule(statement.target, statement.value, pairwise=False, kind="augmented_assignment")
                if isinstance(statement.target, (ast.Attribute, ast.Subscript)):
                    mutation_targets.append((statement.target, statement.value))
                visit_expression(statement.target)
                visit_expression(statement.value)
                continue
            if isinstance(statement, (ast.For, ast.AsyncFor)):
                register_rule(statement.target, statement.iter, pairwise=False, kind="iteration")
                visit_expression(statement.iter)
                visit_statements(statement.body)
                visit_statements(statement.orelse)
                continue
            if isinstance(statement, (ast.With, ast.AsyncWith)):
                for item in statement.items:
                    visit_expression(item.context_expr)
                    if item.optional_vars is not None:
                        register_rule(item.optional_vars, item.context_expr, pairwise=False, kind="context")
                visit_statements(statement.body)
                continue
            if isinstance(statement, ast.Match):
                visit_expression(statement.subject)
                for case in statement.cases:
                    captures = pattern_target(case.pattern)
                    if captures.elts:
                        register_rule(captures, statement.subject, pairwise=False, kind="match_capture")
                    visit_expression(case.guard)
                    visit_statements(case.body)
                continue
            if isinstance(statement, ast.If):
                visit_expression(statement.test)
                visit_statements(statement.body)
                visit_statements(statement.orelse)
                continue
            if isinstance(statement, ast.While):
                visit_expression(statement.test)
                visit_statements(statement.body)
                visit_statements(statement.orelse)
                continue
            if isinstance(statement, ast.Try) or type(statement).__name__ == "TryStar":
                visit_statements(statement.body)
                for handler in statement.handlers:
                    visit_expression(handler.type)
                    if handler.name is not None:
                        register_rule(
                            ast.Name(id=handler.name, ctx=ast.Store()),
                            handler.type,
                            pairwise=False,
                            kind="exception",
                        )
                    visit_statements(handler.body)
                visit_statements(statement.orelse)
                visit_statements(statement.finalbody)
                continue
            for child in ast.iter_child_nodes(statement):
                if isinstance(child, ast.expr):
                    visit_expression(child)

    visit_statements(tree.body)

    def resolve_strings(expression: Any) -> set[str]:
        if expression is None:
            return set()
        if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
            return {expression.value}
        if isinstance(expression, ast.Name) and isinstance(expression.ctx, ast.Load):
            if expression.id == "__name__":
                return {module_name}
            return set(constant_strings.get(expression.id, ()))
        if isinstance(expression, ast.IfExp):
            return resolve_strings(expression.body) | resolve_strings(expression.orelse)
        if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
            left = resolve_strings(expression.left)
            right = resolve_strings(expression.right)
            return {prefix + suffix for prefix in left for suffix in right}
        return set()

    def promote_canonical_origins(value_origins: set[str]) -> set[str]:
        return {
            f"{_V12_MODULE_REGISTRY_PREFIX}sys.modules" if origin == "sys.modules" else origin
            for origin in value_origins
        }

    safe_mapping_members = {"__getitem__", "copy", "get", "items", "keys", "values"}

    def bound_accessor_origin(prefix: str, owner: str, member: str) -> str:
        return f"{prefix}{owner}:{member}"

    def bound_accessor_parts(origin: str, prefix: str) -> tuple[str, str] | None:
        if not origin.startswith(prefix):
            return None
        owner, separator, member = origin.removeprefix(prefix).rpartition(":")
        return (owner, member) if separator else None

    def strip_write_capability(value_origins: set[str]) -> set[str]:
        return {
            origin
            for origin in value_origins
            if origin != _V12_CURRENT_MODULE_NAMESPACE
            and origin != f"{_V12_MODULE_OBJECT_PREFIX}{_V12_CURRENT_MODULE_NAMESPACE}"
            and not origin.startswith(_V12_MODULE_MAPPING_PREFIX)
            and not origin.startswith(_V12_MODULE_REGISTRY_PREFIX)
            and not origin.startswith(_V12_BOUND_SAFE_ACCESSOR_PREFIX)
            and not origin.startswith(_V12_BOUND_NAMESPACE_MUTATOR_PREFIX)
        }

    def is_read_only_call_origin(called: set[str]) -> bool:
        if any(origin.startswith(_V12_BOUND_SAFE_ACCESSOR_PREFIX) for origin in called):
            return True
        if called & {
            "builtins.dict",
            "builtins.frozenset",
            "builtins.getattr",
            "builtins.iter",
            "builtins.len",
            "builtins.list",
            "builtins.set",
            "builtins.sorted",
            "builtins.tuple",
            "operator.getitem",
        }:
            return True
        return False

    def selected_member_origin(owner: str, member: str) -> str:
        canonical_owner = owner.removeprefix(_V12_MODULE_MAPPING_PREFIX).removeprefix(_V12_MODULE_OBJECT_PREFIX)
        if canonical_owner == _V12_CURRENT_MODULE_NAMESPACE:
            return f"<reflective-member>.{member}"
        return f"{canonical_owner}.{member}"

    def resolve_module_registry_lookup(registry_origins: set[str], key: Any) -> set[str]:
        registry_origins = promote_canonical_origins(registry_origins)
        if not any(origin.startswith(_V12_MODULE_REGISTRY_PREFIX) for origin in registry_origins):
            return set()
        selected_modules = resolve_strings(key)
        resolved: set[str] = set()
        for selected_module in selected_modules:
            if selected_module == module_name:
                resolved.update(
                    (
                        _V12_CURRENT_MODULE_NAMESPACE,
                        f"{_V12_MODULE_OBJECT_PREFIX}{_V12_CURRENT_MODULE_NAMESPACE}",
                    )
                )
            else:
                resolved.update((selected_module, f"{_V12_MODULE_OBJECT_PREFIX}{selected_module}"))
        return resolved

    def resolve_origins(expression: Any) -> set[str]:
        if expression is None:
            return set()
        if isinstance(expression, ast.Name) and isinstance(expression.ctx, ast.Load):
            observed = set(origins.get(expression.id, ()))
            if not observed and expression.id in {
                "dict",
                "exec",
                "frozenset",
                "getattr",
                "globals",
                "iter",
                "len",
                "list",
                "set",
                "setattr",
                "sorted",
                "tuple",
                "vars",
            }:
                observed.add(f"builtins.{expression.id}")
            return promote_canonical_origins(observed)
        if isinstance(expression, ast.Attribute):
            owners = resolve_origins(expression.value)
            if expression.attr == "modules" and ("sys" in owners or f"{_V12_MODULE_OBJECT_PREFIX}sys" in owners):
                return {f"{_V12_MODULE_REGISTRY_PREFIX}sys.modules"}
            registry_owners = {owner for owner in owners if owner.startswith(_V12_MODULE_REGISTRY_PREFIX)}
            if registry_owners:
                prefix = (
                    _V12_BOUND_SAFE_ACCESSOR_PREFIX
                    if expression.attr in safe_mapping_members
                    else _V12_BOUND_NAMESPACE_MUTATOR_PREFIX
                )
                return {bound_accessor_origin(prefix, owner, expression.attr) for owner in registry_owners}
            mapping_owners = {
                owner.removeprefix(_V12_MODULE_MAPPING_PREFIX)
                for owner in owners
                if owner.startswith(_V12_MODULE_MAPPING_PREFIX)
            }
            if mapping_owners:
                prefix = (
                    _V12_BOUND_SAFE_ACCESSOR_PREFIX
                    if expression.attr in safe_mapping_members
                    else _V12_BOUND_NAMESPACE_MUTATOR_PREFIX
                )
                return {bound_accessor_origin(prefix, owner, expression.attr) for owner in mapping_owners}
            if expression.attr == "__dict__":
                module_mappings = {
                    f"{_V12_MODULE_MAPPING_PREFIX}{owner.removeprefix(_V12_MODULE_OBJECT_PREFIX)}"
                    for owner in owners
                    if owner.startswith(_V12_MODULE_OBJECT_PREFIX)
                }
                if module_mappings:
                    return module_mappings
            observed = owners | {f"{owner}.{expression.attr}" for owner in owners}
            return observed
        if isinstance(expression, ast.Lambda):
            return {
                origin
                for default in (*expression.args.defaults, *expression.args.kw_defaults)
                if default is not None
                for origin in resolve_origins(default)
            }
        if isinstance(expression, ast.Call):
            called = resolve_origins(expression.func)
            if called & {"builtins.globals", "builtins.vars"} and not expression.args and not expression.keywords:
                return {f"{_V12_MODULE_MAPPING_PREFIX}{_V12_CURRENT_MODULE_NAMESPACE}"}
            if called & {"builtins.vars"} and len(expression.args) == 1 and not expression.keywords:
                module_mappings = {
                    f"{_V12_MODULE_MAPPING_PREFIX}{owner.removeprefix(_V12_MODULE_OBJECT_PREFIX)}"
                    for owner in resolve_origins(expression.args[0])
                    if owner.startswith(_V12_MODULE_OBJECT_PREFIX)
                }
                if module_mappings:
                    return module_mappings
            if called & {"operator.getitem"} and len(expression.args) >= 2:
                registry_lookup = resolve_module_registry_lookup(
                    resolve_origins(expression.args[0]),
                    expression.args[1],
                )
                if registry_lookup:
                    return registry_lookup
            observed = {origin for child in ast.iter_child_nodes(expression) for origin in resolve_origins(child)}
            selected: set[str] = set()
            if called & {"builtins.getattr"} and len(expression.args) >= 2:
                owners = resolve_origins(expression.args[0])
                members = resolve_strings(expression.args[1])
                mapping_owners = {
                    owner.removeprefix(_V12_MODULE_MAPPING_PREFIX)
                    for owner in owners
                    if owner.startswith(_V12_MODULE_MAPPING_PREFIX)
                }
                if mapping_owners:
                    if not members:
                        members = {"*"}
                    return {
                        bound_accessor_origin(
                            _V12_BOUND_SAFE_ACCESSOR_PREFIX
                            if member in safe_mapping_members
                            else _V12_BOUND_NAMESPACE_MUTATOR_PREFIX,
                            owner,
                            member,
                        )
                        for owner in mapping_owners
                        for member in members
                    }
                if "__dict__" in members:
                    module_mappings = {
                        f"{_V12_MODULE_MAPPING_PREFIX}{owner.removeprefix(_V12_MODULE_OBJECT_PREFIX)}"
                        for owner in owners
                        if owner.startswith(_V12_MODULE_OBJECT_PREFIX)
                    }
                    if module_mappings:
                        return module_mappings
                selected.update(selected_member_origin(owner, member) for owner in owners for member in members)
                if not owners:
                    selected.update(f"<reflective-member>.{member}" for member in members)
                return selected or strip_write_capability(observed)
            bound_safe_accessors = {
                parts
                for origin in called
                if (parts := bound_accessor_parts(origin, _V12_BOUND_SAFE_ACCESSOR_PREFIX)) is not None
            }
            if expression.args:
                registry_lookup = {
                    selected_origin
                    for owner, member in bound_safe_accessors
                    if owner.startswith(_V12_MODULE_REGISTRY_PREFIX) and member in {"get", "__getitem__"}
                    for selected_origin in resolve_module_registry_lookup({owner}, expression.args[0])
                }
                if registry_lookup:
                    return registry_lookup
            mapping_owners = {owner for owner, _member in bound_safe_accessors}
            evaluated_arguments = (*expression.args, *(keyword.value for keyword in expression.keywords))
            member_arguments: tuple[Any, ...] = evaluated_arguments
            if called & {"operator.getitem"} and expression.args:
                mapping_owners.update(
                    origin.removeprefix(_V12_MODULE_MAPPING_PREFIX)
                    for origin in resolve_origins(expression.args[0])
                    if origin.startswith(_V12_MODULE_MAPPING_PREFIX)
                )
                member_arguments = (*expression.args[1:], *(keyword.value for keyword in expression.keywords))
            for argument in evaluated_arguments:
                mapping_owners.update(
                    origin.removeprefix(_V12_MODULE_MAPPING_PREFIX)
                    for origin in resolve_origins(argument)
                    if origin.startswith(_V12_MODULE_MAPPING_PREFIX)
                )
            members = {member for argument in member_arguments for member in resolve_strings(argument)}
            for owner in mapping_owners:
                selected.update(selected_member_origin(owner, member) for member in members)
            selected_bound_accessor = any(member in {"get", "__getitem__"} for _owner, member in bound_safe_accessors)
            if selected and (called & {"operator.getitem"} or selected_bound_accessor):
                return selected
            if is_read_only_call_origin(called):
                return strip_write_capability(observed | selected)
            return observed | selected
        if isinstance(expression, ast.Subscript):
            observed = resolve_origins(expression.value) | resolve_origins(expression.slice)
            members = resolve_strings(expression.slice)
            value_origins = resolve_origins(expression.value)
            registry_lookup = resolve_module_registry_lookup(value_origins, expression.slice)
            if registry_lookup:
                return registry_lookup
            if (
                "sys.modules" in value_origins
                and isinstance(expression.slice, ast.Name)
                and expression.slice.id == "__name__"
            ):
                return {
                    _V12_CURRENT_MODULE_NAMESPACE,
                    f"{_V12_MODULE_OBJECT_PREFIX}{_V12_CURRENT_MODULE_NAMESPACE}",
                }
            mapping_modules = {
                origin.removeprefix(_V12_MODULE_MAPPING_PREFIX)
                for origin in value_origins
                if origin.startswith(_V12_MODULE_MAPPING_PREFIX)
            }
            if mapping_modules:
                if members:
                    return {selected_member_origin(owner, member) for owner in mapping_modules for member in members}
                return strip_write_capability(observed)
            for member in members:
                observed.add(f"<reflective-member>.{member}")
            return observed
        if isinstance(expression, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)):
            produced = (expression.key, expression.value) if isinstance(expression, ast.DictComp) else (expression.elt,)
            evaluated = (
                *produced,
                *(generator.iter for generator in expression.generators),
                *(condition for generator in expression.generators for condition in generator.ifs),
            )
            return {origin for item in evaluated for origin in resolve_origins(item)}
        return {
            origin
            for child in ast.iter_child_nodes(expression)
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            for origin in resolve_origins(child)
        }

    def propagate(target: Any, value: Any, pairwise: bool) -> tuple[tuple[str, set[str]], ...]:
        if isinstance(target, ast.Name):
            return ((target.id, resolve_origins(value)),)
        if pairwise and isinstance(target, (ast.Tuple, ast.List)):
            if isinstance(value, ast.IfExp):
                return (*propagate(target, value.body, True), *propagate(target, value.orelse, True))
            if (
                isinstance(value, (ast.Tuple, ast.List))
                and len(target.elts) == len(value.elts)
                and not any(isinstance(element, ast.Starred) for element in target.elts)
            ):
                return tuple(
                    item
                    for target_element, value_element in zip(target.elts, value.elts, strict=True)
                    for item in propagate(target_element, value_element, True)
                )
        conservative = resolve_origins(value)
        return tuple((name, set(conservative)) for name in _v12_target_names(target))

    def propagate_strings(target: Any, value: Any, pairwise: bool) -> tuple[tuple[str, set[str]], ...]:
        if isinstance(target, ast.Name):
            return ((target.id, resolve_strings(value)),)
        if pairwise and isinstance(target, (ast.Tuple, ast.List)):
            if isinstance(value, ast.IfExp):
                return (
                    *propagate_strings(target, value.body, True),
                    *propagate_strings(target, value.orelse, True),
                )
            if (
                isinstance(value, (ast.Tuple, ast.List))
                and len(target.elts) == len(value.elts)
                and not any(isinstance(element, ast.Starred) for element in target.elts)
            ):
                return tuple(
                    item
                    for target_element, value_element in zip(target.elts, value.elts, strict=True)
                    for item in propagate_strings(target_element, value_element, True)
                )
        conservative = {member for child in ast.iter_child_nodes(value) for member in resolve_strings(child)}
        return tuple((name, set(conservative)) for name in _v12_target_names(target))

    def attach_dynamic_module_binding(values: tuple[Any, ...]) -> bool:
        discovered = {origin for value in values for origin in resolve_origins(value)}
        kinds.setdefault(_V12_DYNAMIC_MODULE_BINDING, set()).add("dynamic_module_binding")
        before = len(origins.setdefault(_V12_DYNAMIC_MODULE_BINDING, set()))
        origins[_V12_DYNAMIC_MODULE_BINDING].update((*discovered, _V12_DYNAMIC_MODULE_BINDING))
        return len(origins[_V12_DYNAMIC_MODULE_BINDING]) != before

    def attach_unconditional_dynamic_module_binding() -> bool:
        kinds.setdefault(_V12_DYNAMIC_MODULE_BINDING, set()).add("dynamic_module_binding")
        before = len(origins.setdefault(_V12_DYNAMIC_MODULE_BINDING, set()))
        origins[_V12_DYNAMIC_MODULE_BINDING].add(_V12_DYNAMIC_MODULE_BINDING)
        return len(origins[_V12_DYNAMIC_MODULE_BINDING]) != before

    def has_module_namespace_or_mapping(expression: Any) -> bool:
        return any(
            origin
            in {
                _V12_CURRENT_MODULE_NAMESPACE,
                f"{_V12_MODULE_OBJECT_PREFIX}{_V12_CURRENT_MODULE_NAMESPACE}",
            }
            or origin.startswith(_V12_MODULE_MAPPING_PREFIX)
            or origin.startswith(_V12_MODULE_REGISTRY_PREFIX)
            or origin.startswith(_V12_BOUND_NAMESPACE_MUTATOR_PREFIX)
            for origin in resolve_origins(expression)
        )

    def is_read_only_mapping_accessor(call: Any) -> bool:
        return is_read_only_call_origin(resolve_origins(call.func))

    for _iteration in range(len(rules) + len(calls) + len(origins) + 1):
        changed = False
        for target, value, pairwise in rules:
            for name, value_origins in propagate(target, value, pairwise):
                before = len(origins.setdefault(name, set()))
                origins[name].update(value_origins)
                changed = changed or len(origins[name]) != before
            for name, strings in propagate_strings(target, value, pairwise):
                before = len(constant_strings.setdefault(name, set()))
                constant_strings[name].update(strings)
                changed = changed or len(constant_strings[name]) != before
        for target, value in mutation_targets:
            owner = target.value
            if has_module_namespace_or_mapping(owner):
                changed = attach_dynamic_module_binding((value,)) or changed
        for call in calls:
            called = resolve_origins(call.func)
            dynamic_values: tuple[Any, ...] = ()
            evaluated = (call.func, *call.args, *(keyword.value for keyword in call.keywords))
            if called & {"builtins.exec"}:
                changed = attach_unconditional_dynamic_module_binding() or changed
            elif any(has_module_namespace_or_mapping(item) for item in evaluated):
                if not is_read_only_mapping_accessor(call):
                    dynamic_values = evaluated
            if dynamic_values:
                changed = attach_dynamic_module_binding(dynamic_values) or changed
        if not changed:
            break
    return tuple(
        sorted(
            (name, kind, origin)
            for name, binding_kinds in kinds.items()
            for kind in binding_kinds
            for origin in promote_canonical_origins(origins.get(name) or {f"<unresolved>.{name}"})
        )
    )


def _v12_sensitive_origin(origin: str, sensitive_origins: set[str]) -> bool:
    sensitive_members = {candidate.rpartition(".")[2] for candidate in sensitive_origins}
    return (
        origin == _V12_DYNAMIC_MODULE_BINDING
        or origin in sensitive_origins
        or (origin.startswith("<reflective-member>.") and origin.rpartition(".")[2] in sensitive_members)
    )


def _v12_sensitive_reexports(
    tree: Any,
    module_name: str,
    sensitive_origins: set[str],
) -> tuple[tuple[str, str], ...]:
    import ast

    provenance = _v12_top_level_provenance(tree, module_name)
    declared_exports = _v12_literal_all(tree)
    if declared_exports is not None:
        exports = set(declared_exports)
    elif any(local == "__all__" for local, _kind, _origin in provenance):
        exports = {"<nonliteral-__all__>"}
    else:
        exports = {local for local, _kind, _origin in provenance if not local.startswith("_")}
    external_sensitive = {
        (local, origin)
        for local, _kind, origin in provenance
        if _v12_sensitive_origin(origin, sensitive_origins) and not origin.startswith(f"{module_name}.")
    }
    reexports = {
        (local, origin)
        for local, origin in external_sensitive
        if local in exports or local == _V12_DYNAMIC_MODULE_BINDING
    }
    if "<nonliteral-__all__>" in exports:
        reexports.update(("<nonliteral-__all__>", origin) for _local, origin in external_sensitive)
    for node in _v12_same_scope_nodes(tree.body):
        if not (isinstance(node, ast.ImportFrom) and any(imported.name == "*" for imported in node.names)):
            continue
        imported_module = _v12_import_from_module(node, module_name)
        reexports.update(
            ("*", origin)
            for origin in sensitive_origins
            if origin.rpartition(".")[0] == imported_module and not origin.startswith(f"{module_name}.")
        )
    return tuple(sorted(reexports))


def _v12_literal_all(tree: Any) -> tuple[str, ...] | None:
    import ast

    for statement in tree.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = statement.targets if isinstance(statement, ast.Assign) else (statement.target,)
        if not any(isinstance(target, ast.Name) and target.id == "__all__" for target in targets):
            continue
        if not isinstance(statement.value, (ast.List, ast.Tuple)):
            return None
        values = statement.value.elts
        if all(isinstance(value, ast.Constant) and isinstance(value.value, str) for value in values):
            return tuple(ast.literal_eval(value) for value in values)
        return None
    return None


def _v12_wildcard_exports(tree: Any) -> tuple[str, ...]:
    declared = _v12_literal_all(tree)
    if declared is not None:
        return declared
    if any(name == "__all__" for name, _kind in _v12_top_level_bindings(tree)):
        return ("<nonliteral-__all__>",)
    return tuple(sorted({name for name, _kind in _v12_top_level_bindings(tree) if not name.startswith("_")}))


def _v12_resolve(expression: Any, aliases: dict[str, str]) -> str | None:
    import ast

    if isinstance(expression, ast.Name):
        return aliases.get(expression.id, expression.id)
    if isinstance(expression, ast.Attribute):
        owner = _v12_resolve(expression.value, aliases)
        return None if owner is None else f"{owner}.{expression.attr}"
    return None


def _v12_constant_string(expression: Any, constants: dict[str, str]) -> str | None:
    import ast

    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return expression.value
    if isinstance(expression, ast.Name):
        return constants.get(expression.id)
    return None


def _v12_collect_aliases(
    statements: list[Any],
    initial_aliases: dict[str, str] | None = None,
    initial_constants: dict[str, str] | None = None,
    *,
    current_module: str | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    import ast

    aliases = dict(initial_aliases or {})
    constants = dict(initial_constants or {})
    nodes = _v12_same_scope_nodes(statements)
    for _iteration in range(len(nodes) + 1):
        changed = False
        for node in nodes:
            if isinstance(node, ast.ImportFrom):
                imported_module = (
                    _v12_import_from_module(node, current_module) if current_module is not None else node.module
                )
                if imported_module is None:
                    continue
                for imported in node.names:
                    if imported.name == "*":
                        continue
                    local = imported.asname or imported.name
                    resolved = f"{imported_module}.{imported.name}"
                    if aliases.get(local) != resolved:
                        aliases[local] = resolved
                        changed = True
                continue
            if isinstance(node, ast.Import):
                for imported in node.names:
                    if imported.asname is not None:
                        local, resolved = imported.asname, imported.name
                    else:
                        local = resolved = imported.name.split(".", 1)[0]
                    if aliases.get(local) != resolved:
                        aliases[local] = resolved
                        changed = True
                continue
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            names = tuple(name for target in targets for name in _v12_target_names(target))
            value = node.value
            resolved_alias = _v12_resolve(value, aliases)
            resolved_constant = _v12_constant_string(value, constants)
            if isinstance(value, ast.Call):
                called = _v12_resolve(value.func, aliases)
                first = _v12_constant_string(value.args[0], constants) if value.args else None
                if called in {"importlib.import_module", "__import__", "builtins.__import__"}:
                    resolved_alias = first
                elif called in {"getattr", "builtins.getattr"} and len(value.args) >= 2:
                    owner = _v12_resolve(value.args[0], aliases)
                    attribute = _v12_constant_string(value.args[1], constants)
                    resolved_alias = f"{owner}.{attribute}" if owner is not None and attribute is not None else None
            for name in names:
                if resolved_alias is not None and aliases.get(name) != resolved_alias:
                    aliases[name] = resolved_alias
                    changed = True
                if resolved_constant is not None and constants.get(name) != resolved_constant:
                    constants[name] = resolved_constant
                    changed = True
        if not changed:
            break
    return aliases, constants


def _v12_assigned_call_name(tree: Any, class_name: str, attribute: str) -> str | None:
    import ast

    module_aliases, module_constants = _v12_collect_aliases(tree.body)
    for statement in tree.body:
        if not isinstance(statement, ast.ClassDef) or statement.name != class_name:
            continue
        initializer = next(
            (
                item
                for item in statement.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == "__init__"
            ),
            None,
        )
        if initializer is None:
            return None
        aliases, _constants = _v12_collect_aliases(initializer.body, module_aliases, module_constants)
        for node in _v12_same_scope_nodes(initializer.body):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)) or not isinstance(node.value, ast.Call):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            if any(
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
                and target.attr == attribute
                for target in targets
            ):
                return _v12_resolve(node.value.func, aliases)
    return None


def _v12_factory_returns_own_constructor(function: Any, declared_return: str) -> tuple[int, bool]:
    import ast

    constructor_nodes: set[int] = set()
    returns: list[bool] = []
    supported = True

    def owns(value: Any, state: set[str]) -> bool:
        direct = isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == declared_return
        if direct:
            constructor_nodes.add(id(value))
        return direct or (isinstance(value, ast.Name) and value.id in state)

    def clear_targets(states: list[set[str]], target: Any) -> list[set[str]]:
        names = _v12_target_names(target)
        for state in states:
            state.difference_update(names)
        return states

    def pattern_names(pattern: Any) -> tuple[str, ...]:
        return tuple(node.name for node in ast.walk(pattern) if isinstance(node, ast.MatchAs) and node.name is not None)

    def has_same_scope_return(node: Any) -> bool:
        return any(isinstance(child, ast.Return) for child in _v12_same_scope_nodes([node]))

    def named_expression_targets(expression: Any) -> tuple[str, ...]:
        names: list[str] = []

        def inspect(node: Any) -> None:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                return
            if isinstance(node, ast.NamedExpr):
                names.extend(_v12_target_names(node.target))
            for child in ast.iter_child_nodes(node):
                inspect(child)

        if expression is not None:
            inspect(expression)
        return tuple(names)

    def reject_named_rebindings(states: list[set[str]], *expressions: Any) -> None:
        nonlocal supported
        rebound = {name for expression in expressions for name in named_expression_targets(expression)}
        if not rebound:
            return
        supported = False
        for state in states:
            state.difference_update(rebound)

    def visit(statements: list[Any], states: list[set[str]]) -> list[set[str]]:
        nonlocal supported
        for statement in statements:
            if not states:
                break
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                header_expressions: list[Any] = [*statement.decorator_list]
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    header_expressions.extend(statement.args.defaults)
                    header_expressions.extend(default for default in statement.args.kw_defaults if default is not None)
                    header_expressions.append(statement.returns)
                else:
                    header_expressions.extend(statement.bases)
                    header_expressions.extend(keyword.value for keyword in statement.keywords)
                reject_named_rebindings(states, *header_expressions)
                clear_targets(states, ast.Name(id=statement.name))
                continue
            if isinstance(statement, ast.If):
                reject_named_rebindings(states, statement.test)
                if isinstance(statement.test, ast.Constant) and statement.test.value is False:
                    states = visit(statement.orelse, [set(state) for state in states])
                elif isinstance(statement.test, ast.Constant) and statement.test.value is True:
                    states = visit(statement.body, [set(state) for state in states])
                else:
                    body_states = visit(statement.body, [set(state) for state in states])
                    else_states = (
                        visit(statement.orelse, [set(state) for state in states])
                        if statement.orelse
                        else [set(state) for state in states]
                    )
                    states = body_states + else_states
                continue
            if isinstance(statement, (ast.For, ast.AsyncFor)):
                reject_named_rebindings(states, statement.iter)
                zero_iteration = [set(state) for state in states]
                one_iteration = clear_targets([set(state) for state in states], statement.target)
                body_states = visit(statement.body, one_iteration)
                normal_states = zero_iteration + body_states
                states = visit(statement.orelse, normal_states) if statement.orelse else normal_states
                continue
            if isinstance(statement, ast.While):
                reject_named_rebindings(states, statement.test)
                if isinstance(statement.test, ast.Constant) and statement.test.value is False:
                    states = visit(statement.orelse, [set(state) for state in states])
                else:
                    body_states = visit(statement.body, [set(state) for state in states])
                    normal_states = body_states
                    if not (isinstance(statement.test, ast.Constant) and statement.test.value is True):
                        normal_states += [set(state) for state in states]
                    states = visit(statement.orelse, normal_states) if statement.orelse else normal_states
                continue
            if isinstance(statement, (ast.With, ast.AsyncWith)):
                reject_named_rebindings(states, *(item.context_expr for item in statement.items))
                entered_states = [set(state) for state in states]
                for item in statement.items:
                    if item.optional_vars is not None:
                        clear_targets(entered_states, item.optional_vars)
                states = visit(statement.body, entered_states)
                continue
            if isinstance(statement, ast.Match):
                reject_named_rebindings(states, statement.subject)
                case_states: list[set[str]] = []
                has_irrefutable_case = False
                for case in statement.cases:
                    guarded_states = [set(state) for state in states]
                    reject_named_rebindings(guarded_states, case.guard)
                    if isinstance(case.guard, ast.Constant) and case.guard.value is False:
                        continue
                    branch = guarded_states
                    for state in branch:
                        state.difference_update(pattern_names(case.pattern))
                    case_states += visit(case.body, branch)
                    has_irrefutable_case = has_irrefutable_case or (
                        case.guard is None and isinstance(case.pattern, ast.MatchAs) and case.pattern.pattern is None
                    )
                if not has_irrefutable_case:
                    case_states += [set(state) for state in states]
                states = case_states
                continue
            if isinstance(statement, (ast.Assign, ast.AnnAssign)):
                targets = statement.targets if isinstance(statement, ast.Assign) else (statement.target,)
                names = tuple(name for target in targets for name in _v12_target_names(target))
                value = statement.value
                reject_named_rebindings(states, value)
                for state in states:
                    owns_value = owns(value, state)
                    for name in names:
                        if owns_value:
                            state.add(name)
                        else:
                            state.discard(name)
                continue
            if isinstance(statement, ast.Return):
                value = statement.value
                reject_named_rebindings(states, value)
                returns.extend(owns(value, state) for state in states)
                states = []
            elif isinstance(statement, ast.Raise):
                reject_named_rebindings(states, statement.exc, statement.cause)
                states = []
            elif isinstance(statement, ast.Try) or type(statement).__name__ == "TryStar":
                incoming = [set(state) for state in states]
                body_states = visit(statement.body, [set(state) for state in incoming])
                normal_states = visit(statement.orelse, body_states) if statement.orelse else body_states
                handler_states: list[set[str]] = []
                for handler in statement.handlers:
                    admitted = [set(state) for state in incoming]
                    reject_named_rebindings(admitted, handler.type)
                    if handler.name is not None:
                        clear_targets(admitted, ast.Name(id=handler.name))
                    handler_states += visit(handler.body, admitted)
                states = normal_states + handler_states
                if statement.finalbody:
                    # A finally suite runs for normal, exceptional and pending
                    # return paths.  Traversing it from both live and incoming
                    # states is intentionally conservative and fail closed.
                    states = visit(statement.finalbody, states + [set(state) for state in incoming])
            elif isinstance(statement, ast.AugAssign):
                reject_named_rebindings(states, statement.value)
                states = clear_targets(states, statement.target)
            elif isinstance(statement, ast.Delete):
                for target in statement.targets:
                    clear_targets(states, target)
            elif isinstance(statement, (ast.Break, ast.Continue)):
                supported = False
                states = []
            elif isinstance(statement, ast.Import):
                for imported in statement.names:
                    clear_targets(states, ast.Name(id=imported.asname or imported.name.split(".", 1)[0]))
            elif isinstance(statement, ast.ImportFrom):
                for imported in statement.names:
                    if imported.name != "*":
                        clear_targets(states, ast.Name(id=imported.asname or imported.name))
            elif isinstance(statement, ast.Expr):
                reject_named_rebindings(states, statement.value)
            elif isinstance(statement, ast.Assert):
                reject_named_rebindings(states, statement.test, statement.msg)
            elif isinstance(statement, (ast.Global, ast.Nonlocal)):
                supported = False
            elif isinstance(statement, ast.Pass):
                pass
            elif has_same_scope_return(statement):
                supported = False
                states = []
        return states

    visit(function.body, [set()])
    return len(constructor_nodes), supported and bool(returns) and all(returns)


def _v12_symbol_graph(
    trees: tuple[tuple[Path, Any], ...],
    symbols: tuple[str, ...],
    *,
    boundary_path: Path,
) -> tuple[dict[str, tuple[Path, ...]], tuple[tuple[Path, str], ...], tuple[tuple[Path, str], ...]]:
    import ast

    canonical = {symbol: f"{_V12_BOUNDARY_MODULE}.{symbol}" for symbol in symbols}
    callers: dict[str, set[Path]] = {symbol: set() for symbol in symbols}
    reexports: set[tuple[Path, str]] = set()
    dynamic: set[tuple[Path, str]] = set()
    dynamic_functions = {"importlib.import_module", "__import__", "builtins.__import__", "getattr", "builtins.getattr"}

    for path, tree in trees:
        current_module = _v12_module_name(path)
        module_aliases, module_constants = _v12_collect_aliases(
            tree.body,
            current_module=current_module,
        )
        lexical_module_nodes = _v12_same_scope_nodes(tree.body)
        boundary_star_import = any(
            isinstance(node, ast.ImportFrom)
            and _v12_import_from_module(node, current_module) == _V12_BOUNDARY_MODULE
            and any(imported.name == "*" for imported in node.names)
            for node in lexical_module_nodes
        )
        if boundary_star_import and path != boundary_path:
            reexports.add((path, "*"))
        exports = set(_v12_wildcard_exports(tree))
        has_sensitive_alias = any(resolved in canonical.values() for resolved in module_aliases.values())
        if (
            "<nonliteral-__all__>" in exports
            and (boundary_star_import or has_sensitive_alias)
            and path != boundary_path
        ):
            reexports.add((path, "<nonliteral-__all__>"))
        for exported in exports:
            resolved = module_aliases.get(exported)
            for symbol, target in canonical.items():
                if resolved == target and path != boundary_path:
                    reexports.add((path, symbol))

        def inspect_scope(
            statements: list[Any],
            inherited_aliases: dict[str, str],
            inherited_constants: dict[str, str],
        ) -> None:
            aliases, constants = _v12_collect_aliases(
                statements,
                inherited_aliases,
                inherited_constants,
                current_module=current_module,
            )
            lexical_nodes = _v12_same_scope_nodes(statements)
            for call in (node for node in lexical_nodes if isinstance(node, ast.Call)):
                resolved = _v12_resolve(call.func, aliases)
                for symbol, target in canonical.items():
                    if resolved == target and path != boundary_path:
                        callers[symbol].add(path)
                if resolved not in dynamic_functions:
                    continue
                argument_strings = {
                    value
                    for argument in (*call.args, *(keyword.value for keyword in call.keywords))
                    if (value := _v12_constant_string(argument, constants)) is not None
                }
                first_argument = _v12_resolve(call.args[0], aliases) if call.args else None
                if (
                    _V12_BOUNDARY_MODULE in argument_strings
                    or argument_strings & set(symbols)
                    or first_argument == _V12_BOUNDARY_MODULE
                ):
                    dynamic.add((path, resolved))
            for nested in (
                node
                for node in lexical_nodes
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            ):
                inspect_scope(nested.body, aliases, constants)

        inspect_scope(tree.body, module_aliases, module_constants)
    return (
        {symbol: tuple(sorted(paths)) for symbol, paths in callers.items()},
        tuple(sorted(reexports)),
        tuple(sorted(dynamic)),
    )


def _v12_internal_factory_callers(tree: Any, symbols: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    import ast

    module_aliases = {symbol: f"{_V12_BOUNDARY_MODULE}.{symbol}" for symbol in symbols}
    module_aliases, module_constants = _v12_collect_aliases(
        tree.body,
        module_aliases,
        current_module=_V12_BOUNDARY_MODULE,
    )
    callers: set[tuple[str, str]] = set()

    def inspect_scope(
        statements: list[Any],
        inherited_aliases: dict[str, str],
        inherited_constants: dict[str, str],
    ) -> None:
        aliases, constants = _v12_collect_aliases(
            statements,
            inherited_aliases,
            inherited_constants,
            current_module=_V12_BOUNDARY_MODULE,
        )
        lexical_nodes = _v12_same_scope_nodes(statements)
        for function in (node for node in lexical_nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
            function_aliases, function_constants = _v12_collect_aliases(
                function.body,
                aliases,
                constants,
                current_module=_V12_BOUNDARY_MODULE,
            )
            for call in (node for node in _v12_same_scope_nodes(function.body) if isinstance(node, ast.Call)):
                resolved = _v12_resolve(call.func, function_aliases)
                for symbol in symbols:
                    if resolved == f"{_V12_BOUNDARY_MODULE}.{symbol}" and function.name != symbol:
                        callers.add((function.name, symbol))
            inspect_scope(function.body, function_aliases, function_constants)
        for klass in (node for node in lexical_nodes if isinstance(node, ast.ClassDef)):
            inspect_scope(klass.body, aliases, constants)

    inspect_scope(tree.body, module_aliases, module_constants)
    return tuple(sorted(callers))


def test_v12_connector_and_runtime_exports_and_reservation_locks_match_literal_golden() -> None:
    import ast

    root = Path("src/dpone")
    connector_path = root / "runtime/connectors/postgres.py"
    runtime_path = root / "runtime/postgres_mssql_source_schema_runtime.py"
    snapshot_path = root / "runtime/sources/postgres_verified_relation_snapshot.py"
    bootstrap_path = root / "runtime/bootstrap_postgres_source_authority.py"
    connector_tree = _v12_parse(connector_path)
    runtime_tree = _v12_parse(runtime_path)
    snapshot_tree = _v12_parse(snapshot_path)
    binder_origin = "dpone.runtime.bootstrap_postgres_source_authority.bind_postgres_mssql_source_schema_runtime"
    runtime_v1_origin = "dpone.runtime.postgres_mssql_source_schema_runtime.PostgresMssqlSourceSchemaRuntimeV1"
    sensitive_origins = {binder_origin, runtime_v1_origin}
    sensitive_names = {
        "PostgresMssqlSourceSchemaRuntime",
        "PostgresMssqlSourceSchemaRuntimeV1",
        "bind_postgres_mssql_source_schema_runtime",
    }
    sensitive_bindings = tuple(
        sorted(
            (path, name, kind, origin)
            for path in root.rglob("*.py")
            for name, kind, origin in _v12_top_level_provenance(_v12_parse(path), _v12_module_name(path))
            if name in sensitive_names or _v12_sensitive_origin(origin, sensitive_origins)
        )
    )
    sensitive_reexports = tuple(
        sorted(
            (path, local, origin)
            for path in root.rglob("*.py")
            for local, origin in _v12_sensitive_reexports(_v12_parse(path), _v12_module_name(path), sensitive_origins)
        )
    )
    analyzer_alias_tree = ast.parse(
        "callable_target = lambda: None\n"
        "if enabled:\n"
        "    bind_postgres_mssql_source_schema_runtime = callable_target\n"
        "class Holder:\n"
        "    def bind_postgres_mssql_source_schema_runtime(self): pass\n"
    )
    analyzer_default_exports = ast.parse("import json as codec\n_hidden = 1\nclass Public: pass\n")
    analyzer_dynamic_all = ast.parse("__all__ = make_exports()\nclass Public: pass\n")
    analyzer_sensitive_aliases = ast.parse(
        "from dpone.runtime.bootstrap_postgres_source_authority "
        "import bind_postgres_mssql_source_schema_runtime as install\n"
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "RuntimeAlias = Versioned\n"
        "__all__ = ['install', 'RuntimeAlias']\n"
        "class Holder:\n"
        "    def bind_postgres_mssql_source_schema_runtime(self): pass\n"
    )
    analyzer_sensitive_star = ast.parse("from dpone.runtime.postgres_mssql_source_schema_runtime import *\n")
    analyzer_sensitive_dynamic_all = ast.parse(
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "__all__ = make_exports()\n"
    )
    analyzer_relative_aliases = ast.parse(
        "from .bootstrap_postgres_source_authority "
        "import bind_postgres_mssql_source_schema_runtime as install\n"
        "install_chain = install\n"
        "from .postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "RuntimeAlias = Versioned\n"
        "__all__ = ['install', 'RuntimeAlias']\n"
    )
    analyzer_relative_star = ast.parse("from .postgres_mssql_source_schema_runtime import *\n")
    analyzer_destructuring = ast.parse(
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "(RuntimeAlias, [NestedAlias]) = (Versioned, [Versioned])\n"
        "[ListAlias, OtherListAlias] = [Versioned, Versioned]\n"
        "MismatchAlias, mismatch_tail = (Versioned,)\n"
    )
    analyzer_type_aliases = ast.parse(
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "type PostgresMssqlSourceSchemaRuntime = Versioned\n"
        "type RenamedTypeAlias = Versioned\n"
    )
    analyzer_conditional_alias = ast.parse(
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "ConditionalAlias = Versioned if enabled else fallback\n"
    )
    analyzer_generic_expressions = ast.parse(
        "from contextlib import nullcontext\n"
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "import dpone.runtime.postgres_mssql_source_schema_runtime as rt\n"
        "TupleSubscriptAlias = (Versioned,)[0]\n"
        "ListSubscriptAlias = [Versioned][0]\n"
        "DictSubscriptAlias = {'runtime': Versioned}['runtime']\n"
        "BoolAlias = fallback or Versioned\n"
        "(WalrusAlias := Versioned)\n"
        "[(ComprehensionAlias := Versioned) for item in ()]\n"
        "for IterAlias in (Versioned,):\n"
        "    pass\n"
        "with nullcontext(Versioned) as ContextAlias:\n"
        "    pass\n"
        "match Versioned:\n"
        "    case MatchAlias:\n"
        "        pass\n"
        "DunderDictAlias = rt.__dict__['PostgresMssqlSourceSchemaRuntimeV1']\n"
        "VarsAlias = vars(rt)['PostgresMssqlSourceSchemaRuntimeV1']\n"
        "UnknownAlias = opaque({'nested': [Versioned]})\n"
        "async for AsyncIterAlias in (Versioned,):\n"
        "    pass\n"
        "async with nullcontext(Versioned) as AsyncContextAlias:\n"
        "    pass\n"
    )
    analyzer_reflective_calls = ast.parse(
        "import operator\n"
        "import dpone.runtime.postgres_mssql_source_schema_runtime as rt\n"
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "MEMBER = 'PostgresMssqlSourceSchemaRuntimeV1'\n"
        "MEMBER_ALIAS = MEMBER\n"
        "getter = getattr\n"
        "DirectGetattrAlias = getattr(rt, MEMBER_ALIAS)\n"
        "AliasedGetattrAlias = getter(rt, MEMBER_ALIAS)\n"
        "mapping_get = rt.__dict__.get\n"
        "module_dict = rt.__dict__\n"
        "module_dict_alias = module_dict\n"
        "DirectMappingAlias = rt.__dict__.get(MEMBER_ALIAS)\n"
        "AliasedMappingAlias = mapping_get(MEMBER_ALIAS)\n"
        "AliasedDunderMappingAlias = module_dict_alias.get(MEMBER_ALIAS)\n"
        "MappingItemAlias = rt.__dict__.__getitem__(MEMBER_ALIAS)\n"
        "DunderMappingItemAlias = module_dict_alias.__getitem__(MEMBER_ALIAS)\n"
        "module_mapping = vars(rt)\n"
        "module_mapping_alias = module_mapping\n"
        "vars_mapping_get = module_mapping_alias.get\n"
        "VarsMappingAlias = vars(rt).get(MEMBER_ALIAS)\n"
        "AliasedVarsMappingAlias = vars_mapping_get(MEMBER_ALIAS)\n"
        "vars_mapping_item = module_mapping.__getitem__\n"
        "VarsMappingItemAlias = vars_mapping_item(MEMBER_ALIAS)\n"
        "operator_getitem = operator.getitem\n"
        "OperatorMappingAlias = operator.getitem(module_mapping, MEMBER_ALIAS)\n"
        "AliasedOperatorMappingAlias = operator_getitem(module_mapping_alias, MEMBER_ALIAS)\n"
        "OpaqueMappingAccessorAlias = opaque_accessor(module_mapping, MEMBER_ALIAS)\n"
    )
    analyzer_decorated_bindings = ast.parse(
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "@Versioned\n"
        "def DecoratedFunction():\n"
        "    hidden = Versioned\n"
        "@wrapper(Versioned)\n"
        "class DecoratedClass:\n"
        "    hidden = Versioned\n"
    )
    analyzer_dynamic_module_bindings = ast.parse(
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "import sys\n"
        "import operator\n"
        "globals_alias = globals\n"
        "vars_alias = vars\n"
        "namespace = globals_alias()\n"
        "namespace_alias = namespace\n"
        "vars_namespace = vars_alias()\n"
        "modules_alias = sys.modules\n"
        "sys_namespace = modules_alias[__name__]\n"
        "namespace_alias['subscript'] = Versioned\n"
        "vars_namespace.attribute = Versioned\n"
        "sys_namespace['sys'] = Versioned\n"
        "namespace.update({'update': Versioned})\n"
        "updater = namespace.update\n"
        "updater({'aliased_update': Versioned})\n"
        "namespace.__setitem__('item', Versioned)\n"
        "item_setter = namespace.__setitem__\n"
        "item_setter('aliased_item', Versioned)\n"
        "attribute_setter = setattr\n"
        "attribute_setter(namespace, 'attribute', Versioned)\n"
        "globals().setdefault('direct_setdefault', Versioned)\n"
        "globals().set_default('direct_set_default', Versioned)\n"
        "namespace.set_default('set_default', Versioned)\n"
        "set_default_alias = namespace.set_default\n"
        "set_default_alias('aliased_set_default', Versioned)\n"
        "operator.setitem(namespace, 'operator_item', Versioned)\n"
        "operator.setitem(globals(), 'direct_operator_item', Versioned)\n"
        "operator_setitem = operator.setitem\n"
        "operator_setitem(vars_namespace, 'aliased_operator_item', Versioned)\n"
        "custom_publish(namespace_alias, Versioned)\n"
        "custom_publish(globals(), Versioned)\n"
    )
    analyzer_exec_bindings = ast.parse(
        'CODE = "opaque_assignment()"\n'
        "execute = exec\n"
        'exec("PostgresMssqlSourceSchemaRuntime = opaque()")\n'
        "execute(CODE)\n"
        "exec(CODE, globals())\n"
    )
    analyzer_hidden_publish = ast.parse(
        "def make_publisher():\n"
        "    def hidden_publish(namespace):\n"
        "        namespace['hidden'] = object()\n"
        "    return hidden_publish\n"
        "publish = make_publisher()\n"
        "publish(globals())\n"
        "g = globals\n"
        "p = publish\n"
        "p(g())\n"
    )
    analyzer_read_only_mapping = ast.parse(
        "import operator\n"
        "import dpone.runtime.postgres_mssql_source_schema_runtime as rt\n"
        "MEMBER = 'PostgresMssqlSourceSchemaRuntimeV1'\n"
        "mapping = vars(rt)\n"
        "get_alias = mapping.get\n"
        "item_alias = mapping.__getitem__\n"
        "operator_alias = operator.getitem\n"
        "DirectRead = mapping.get(MEMBER)\n"
        "AliasedRead = get_alias(MEMBER)\n"
        "ItemRead = item_alias(MEMBER)\n"
        "OperatorRead = operator_alias(mapping, MEMBER)\n"
        "DunderRead = rt.__dict__.get(MEMBER)\n"
    )
    analyzer_detached_mapping_results = ast.parse(
        "SENSITIVE_MEMBER = 'PostgresMssqlSourceSchemaRuntimeV1'\n"
        "snapshot = globals().copy()\n"
        "snapshot['ordinary'] = 1\n"
        "snapshot_dict = dict(globals())\n"
        "snapshot_dict['ordinary'] = 1\n"
        "ordinary = globals().get('ordinary')\n"
        "consume(ordinary)\n"
        "items_view = globals().items()\n"
        "consume(items_view)\n"
        "keys_view = globals().keys()\n"
        "values_view = globals().values()\n"
        "detached_list = list(globals())\n"
        "detached_tuple = tuple(globals())\n"
        "detached_set = set(globals())\n"
        "detached_frozenset = frozenset(globals())\n"
        "detached_iter = iter(globals())\n"
        "detached_sorted = sorted(globals())\n"
        "detached_len = len(globals())\n"
        "consume(keys_view, values_view, detached_list, detached_tuple)\n"
        "consume(detached_set, detached_frozenset, detached_iter, detached_sorted, detached_len)\n"
        "SelectedSensitive = globals().get(SENSITIVE_MEMBER)\n"
        "consume(SelectedSensitive)\n"
    )
    analyzer_safe_bound_values = ast.parse(
        "reader = globals().get\n"
        "consume(reader)\n"
        "items_reader = globals().items\n"
        "consume(items_reader)\n"
        "keys_reader = globals().keys\n"
        "consume(keys_reader)\n"
        "values_reader = globals().values\n"
        "consume(values_reader)\n"
        "item_reader = globals().__getitem__\n"
        "consume(item_reader)\n"
        "copy_reader = globals().copy\n"
        "consume(copy_reader)\n"
        "getattr_reader = getattr(globals(), 'get')\n"
        "consume(getattr_reader)\n"
    )
    analyzer_typed_namespace_capabilities = ast.parse(
        "import operator\n"
        "import sys\n"
        "update = getattr(globals(), 'update')\n"
        "setter = getattr(globals(), '__setitem__')\n"
        "unknown = getattr(globals(), 'publish')\n"
        "update({})\n"
        "setter('name', object())\n"
        "unknown()\n"
        "modules = sys.modules\n"
        "module_name = __name__\n"
        "module_getter = operator.getitem\n"
        "module = module_getter(modules, module_name)\n"
        "namespace = getattr(module, '__dict__')\n"
        "namespace_direct = module.__dict__\n"
        "publish(namespace)\n"
        "setattr(module, 'name', object())\n"
    )
    analyzer_module_registry_lookup = ast.parse(
        "import operator\n"
        "import sys\n"
        "registry = sys.modules\n"
        "module_name = __name__\n"
        "module_via_get = registry.get(module_name)\n"
        "bound_item = registry.__getitem__\n"
        "module_via_bound = bound_item(module_name)\n"
        "operator_lookup = operator.getitem\n"
        "module_via_operator = operator_lookup(registry, module_name)\n"
        "module_via_subscript = registry[module_name]\n"
        "mapping_from_attribute = module_via_get.__dict__\n"
        "mapping_from_getattr = getattr(module_via_bound, '__dict__')\n"
        "publish(mapping_from_attribute)\n"
        "setattr(module_via_operator, 'published', object())\n"
    )
    analyzer_imported_module_registry = ast.parse(
        "from operator import getitem as operator_lookup\n"
        "from sys import modules as registry\n"
        "name = __name__\n"
        "module_via_subscript = registry[name]\n"
        "module_via_get = registry.get(name)\n"
        "bound_lookup = registry.__getitem__\n"
        "module_via_bound = bound_lookup(name)\n"
        "module_via_operator = operator_lookup(registry, name)\n"
        "setattr(module_via_subscript, 'published', object())\n"
    )
    analyzer_unrelated_mutations = ast.parse(
        "import operator\n"
        "from dpone.runtime.postgres_mssql_source_schema_runtime "
        "import PostgresMssqlSourceSchemaRuntimeV1 as Versioned\n"
        "class Holder:\n"
        "    pass\n"
        "holder = Holder()\n"
        "holder.attribute = Versioned\n"
        "holder.update({'value': Versioned})\n"
        "holder.__setitem__('value', Versioned)\n"
        "setattr(holder, 'value', Versioned)\n"
        "holder.set_default('value', Versioned)\n"
        "operator.setitem(holder, 'value', Versioned)\n"
        "custom_publish(holder, Versioned)\n"
    )

    def synthetic_sensitive(tree: Any, module_name: str) -> tuple[tuple[str, str, str], ...]:
        return tuple(
            item
            for item in _v12_top_level_provenance(tree, module_name)
            if item[0] in sensitive_names or _v12_sensitive_origin(item[2], sensitive_origins)
        )

    analyzer_lock_alias = ast.parse(
        "from threading import Lock as Mutex\nclass Issuer:\n    def __init__(self):\n        self._lock = Mutex()\n"
    )
    terminal_lifecycle_receipt_mutations: set[tuple[str, str]] = set()
    for node in ast.walk(snapshot_tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and (node.func.value.id, node.func.attr) == ("object", "__setattr__")
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value
            in {
                "clock_authority",
                "extraction_completed_at",
                "extraction_started_at",
                "snapshot_acquired_at",
                "snapshot_authority",
                "source_token",
            }
        ):
            terminal_lifecycle_receipt_mutations.add(("object.__setattr__", str(node.args[1].value)))
        if (
            isinstance(node, ast.Attribute)
            and node.attr in {"_lock", "_receipt"}
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == "lifecycle"
        ):
            terminal_lifecycle_receipt_mutations.add(("direct_lifecycle_private_access", node.attr))
    observed = {
        "connector_exports": _v12_wildcard_exports(connector_tree),
        "runtime_exports": _v12_wildcard_exports(runtime_tree),
        "runtime_module_getattr_absent": not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "__getattr__"
            for node in runtime_tree.body
        ),
        "sensitive_top_level_bindings": sensitive_bindings,
        "sensitive_reexports": sensitive_reexports,
        "connector_reservation_constructor": _v12_assigned_call_name(
            connector_tree, "PostgresConnector", "_connection_lock"
        ),
        "issuer_reservation_constructor": _v12_assigned_call_name(
            snapshot_tree, "PostgresVerifiedRelationSnapshotIssuerV1", "_lock"
        ),
        "analyzer_assignment_alias_detected": (
            "bind_postgres_mssql_source_schema_runtime",
            "assignment",
        )
        in _v12_top_level_bindings(analyzer_alias_tree),
        "analyzer_class_method_excluded": sum(
            name == "bind_postgres_mssql_source_schema_runtime"
            for name, _kind in _v12_top_level_bindings(analyzer_alias_tree)
        )
        == 1,
        "analyzer_default_wildcard_exports": _v12_wildcard_exports(analyzer_default_exports),
        "analyzer_nonliteral_all_is_closed": _v12_wildcard_exports(analyzer_dynamic_all),
        "analyzer_sensitive_alias_provenance": synthetic_sensitive(
            analyzer_sensitive_aliases,
            "synthetic.aliases",
        ),
        "analyzer_sensitive_alias_reexports": _v12_sensitive_reexports(
            analyzer_sensitive_aliases,
            "synthetic.aliases",
            sensitive_origins,
        ),
        "analyzer_sensitive_star_reexport": _v12_sensitive_reexports(
            analyzer_sensitive_star,
            "synthetic.star",
            sensitive_origins,
        ),
        "analyzer_sensitive_nonliteral_reexport": _v12_sensitive_reexports(
            analyzer_sensitive_dynamic_all,
            "synthetic.dynamic_all",
            sensitive_origins,
        ),
        "analyzer_relative_alias_provenance": synthetic_sensitive(
            analyzer_relative_aliases,
            "dpone.runtime.synthetic_aliases",
        ),
        "analyzer_relative_alias_reexports": _v12_sensitive_reexports(
            analyzer_relative_aliases,
            "dpone.runtime.synthetic_aliases",
            sensitive_origins,
        ),
        "analyzer_relative_star_reexport": _v12_sensitive_reexports(
            analyzer_relative_star,
            "dpone.runtime.synthetic_star",
            sensitive_origins,
        ),
        "analyzer_pairwise_destructuring": synthetic_sensitive(
            analyzer_destructuring,
            "synthetic.destructuring",
        ),
        "analyzer_type_alias_provenance": synthetic_sensitive(
            analyzer_type_aliases,
            "synthetic.type_aliases",
        ),
        "analyzer_conditional_alias_provenance": synthetic_sensitive(
            analyzer_conditional_alias,
            "synthetic.conditional",
        ),
        "analyzer_generic_expression_provenance": synthetic_sensitive(
            analyzer_generic_expressions,
            "synthetic.generic",
        ),
        "analyzer_generic_expression_reexports": _v12_sensitive_reexports(
            analyzer_generic_expressions,
            "synthetic.generic",
            sensitive_origins,
        ),
        "analyzer_reflective_call_provenance": synthetic_sensitive(
            analyzer_reflective_calls,
            "synthetic.reflective",
        ),
        "analyzer_decorator_provenance": synthetic_sensitive(
            analyzer_decorated_bindings,
            "synthetic.decorated",
        ),
        "analyzer_decorator_reexports": _v12_sensitive_reexports(
            analyzer_decorated_bindings,
            "synthetic.decorated",
            sensitive_origins,
        ),
        "analyzer_dynamic_module_binding": synthetic_sensitive(
            analyzer_dynamic_module_bindings,
            "synthetic.dynamic_module",
        ),
        "analyzer_dynamic_module_reexport": _v12_sensitive_reexports(
            analyzer_dynamic_module_bindings,
            "synthetic.dynamic_module",
            sensitive_origins,
        ),
        "analyzer_exec_dynamic_binding": synthetic_sensitive(
            analyzer_exec_bindings,
            "synthetic.exec_binding",
        ),
        "analyzer_exec_dynamic_reexport": _v12_sensitive_reexports(
            analyzer_exec_bindings,
            "synthetic.exec_binding",
            sensitive_origins,
        ),
        "analyzer_hidden_publish_binding": synthetic_sensitive(
            analyzer_hidden_publish,
            "synthetic.hidden_publish",
        ),
        "analyzer_hidden_publish_reexport": _v12_sensitive_reexports(
            analyzer_hidden_publish,
            "synthetic.hidden_publish",
            sensitive_origins,
        ),
        "analyzer_read_only_mapping_provenance": synthetic_sensitive(
            analyzer_read_only_mapping,
            "synthetic.read_only_mapping",
        ),
        "analyzer_read_only_mapping_has_no_dynamic_binding": not any(
            local == _V12_DYNAMIC_MODULE_BINDING
            for local, _kind, _origin in _v12_top_level_provenance(
                analyzer_read_only_mapping,
                "synthetic.read_only_mapping",
            )
        ),
        "analyzer_detached_mapping_provenance": synthetic_sensitive(
            analyzer_detached_mapping_results,
            "synthetic.detached_mapping",
        ),
        "analyzer_detached_mapping_has_no_dynamic_binding": not any(
            local == _V12_DYNAMIC_MODULE_BINDING
            for local, _kind, _origin in _v12_top_level_provenance(
                analyzer_detached_mapping_results,
                "synthetic.detached_mapping",
            )
        ),
        "analyzer_safe_bound_value_origins": tuple(
            item
            for item in _v12_top_level_provenance(
                analyzer_safe_bound_values,
                "synthetic.safe_bound_values",
            )
            if item[0]
            in {
                "copy_reader",
                "getattr_reader",
                "item_reader",
                "items_reader",
                "keys_reader",
                "reader",
                "values_reader",
            }
        ),
        "analyzer_safe_bound_values_have_no_dynamic_binding": not any(
            local == _V12_DYNAMIC_MODULE_BINDING
            for local, _kind, _origin in _v12_top_level_provenance(
                analyzer_safe_bound_values,
                "synthetic.safe_bound_values",
            )
        ),
        "analyzer_typed_namespace_capability_origins": tuple(
            item
            for item in _v12_top_level_provenance(
                analyzer_typed_namespace_capabilities,
                "synthetic.typed_namespace",
            )
            if item[0] in {"module", "namespace", "namespace_direct", "setter", "unknown", "update"}
        ),
        "analyzer_typed_namespace_mutation_binding": synthetic_sensitive(
            analyzer_typed_namespace_capabilities,
            "synthetic.typed_namespace",
        ),
        "analyzer_typed_namespace_mutation_reexport": _v12_sensitive_reexports(
            analyzer_typed_namespace_capabilities,
            "synthetic.typed_namespace",
            sensitive_origins,
        ),
        "analyzer_module_registry_origins": tuple(
            item
            for item in _v12_top_level_provenance(
                analyzer_module_registry_lookup,
                "synthetic.module_registry",
            )
            if item[0]
            in {
                "mapping_from_attribute",
                "mapping_from_getattr",
                "module_via_bound",
                "module_via_get",
                "module_via_operator",
                "module_via_subscript",
                "registry",
            }
        ),
        "analyzer_module_registry_mutation_binding": synthetic_sensitive(
            analyzer_module_registry_lookup,
            "synthetic.module_registry",
        ),
        "analyzer_imported_registry_origins": tuple(
            item
            for item in _v12_top_level_provenance(
                analyzer_imported_module_registry,
                "synthetic.imported_registry",
            )
            if item[0]
            in {
                "module_via_bound",
                "module_via_get",
                "module_via_operator",
                "module_via_subscript",
                "registry",
            }
        ),
        "analyzer_imported_registry_mutation_binding": synthetic_sensitive(
            analyzer_imported_module_registry,
            "synthetic.imported_registry",
        ),
        "analyzer_unrelated_mutation_has_no_dynamic_binding": not any(
            local == _V12_DYNAMIC_MODULE_BINDING
            for local, _kind, _origin in _v12_top_level_provenance(
                analyzer_unrelated_mutations,
                "synthetic.unrelated",
            )
        ),
        "analyzer_function_local_lock_alias": _v12_assigned_call_name(analyzer_lock_alias, "Issuer", "_lock"),
        "terminal_lifecycle_receipt_mutations": tuple(sorted(terminal_lifecycle_receipt_mutations)),
    }
    literal_golden = {
        "connector_exports": (
            "AbstractConnector",
            "Any",
            "Iterable",
            "Iterator",
            "PostgresConnector",
            "annotations",
            "dict_row",
            "hashlib",
            "psycopg",
            "sql",
            "time",
        ),
        "runtime_exports": ("PostgresMssqlSourceSchemaRuntimeV1",),
        "runtime_module_getattr_absent": True,
        "sensitive_top_level_bindings": (
            (
                bootstrap_path,
                "bind_postgres_mssql_source_schema_runtime",
                "function",
                binder_origin,
            ),
            (
                runtime_path,
                "PostgresMssqlSourceSchemaRuntimeV1",
                "class",
                runtime_v1_origin,
            ),
        ),
        "sensitive_reexports": (),
        "connector_reservation_constructor": "threading.Lock",
        "issuer_reservation_constructor": "threading.Lock",
        "analyzer_assignment_alias_detected": True,
        "analyzer_class_method_excluded": True,
        "analyzer_default_wildcard_exports": ("Public", "codec"),
        "analyzer_nonliteral_all_is_closed": ("<nonliteral-__all__>",),
        "analyzer_sensitive_alias_provenance": (
            ("RuntimeAlias", "assignment", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
            ("install", "import", binder_origin),
        ),
        "analyzer_sensitive_alias_reexports": (
            ("RuntimeAlias", runtime_v1_origin),
            ("install", binder_origin),
        ),
        "analyzer_sensitive_star_reexport": (("*", runtime_v1_origin),),
        "analyzer_sensitive_nonliteral_reexport": (("<nonliteral-__all__>", runtime_v1_origin),),
        "analyzer_relative_alias_provenance": (
            ("RuntimeAlias", "assignment", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
            ("install", "import", binder_origin),
            ("install_chain", "assignment", binder_origin),
        ),
        "analyzer_relative_alias_reexports": (
            ("RuntimeAlias", runtime_v1_origin),
            ("install", binder_origin),
        ),
        "analyzer_relative_star_reexport": (("*", runtime_v1_origin),),
        "analyzer_pairwise_destructuring": (
            ("ListAlias", "assignment", runtime_v1_origin),
            ("MismatchAlias", "assignment", runtime_v1_origin),
            ("NestedAlias", "assignment", runtime_v1_origin),
            ("OtherListAlias", "assignment", runtime_v1_origin),
            ("RuntimeAlias", "assignment", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
            ("mismatch_tail", "assignment", runtime_v1_origin),
        ),
        "analyzer_type_alias_provenance": (
            ("PostgresMssqlSourceSchemaRuntime", "type_alias", runtime_v1_origin),
            ("RenamedTypeAlias", "type_alias", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
        ),
        "analyzer_conditional_alias_provenance": (
            ("ConditionalAlias", "assignment", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
        ),
        "analyzer_generic_expression_provenance": (
            ("AsyncContextAlias", "context", runtime_v1_origin),
            ("AsyncIterAlias", "iteration", runtime_v1_origin),
            ("BoolAlias", "assignment", runtime_v1_origin),
            ("ComprehensionAlias", "named_expression", runtime_v1_origin),
            ("ContextAlias", "context", runtime_v1_origin),
            ("DictSubscriptAlias", "assignment", runtime_v1_origin),
            ("DunderDictAlias", "assignment", runtime_v1_origin),
            ("IterAlias", "iteration", runtime_v1_origin),
            ("ListSubscriptAlias", "assignment", runtime_v1_origin),
            ("MatchAlias", "match_capture", runtime_v1_origin),
            ("TupleSubscriptAlias", "assignment", runtime_v1_origin),
            ("UnknownAlias", "assignment", runtime_v1_origin),
            ("VarsAlias", "assignment", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
            ("WalrusAlias", "named_expression", runtime_v1_origin),
        ),
        "analyzer_generic_expression_reexports": (
            ("AsyncContextAlias", runtime_v1_origin),
            ("AsyncIterAlias", runtime_v1_origin),
            ("BoolAlias", runtime_v1_origin),
            ("ComprehensionAlias", runtime_v1_origin),
            ("ContextAlias", runtime_v1_origin),
            ("DictSubscriptAlias", runtime_v1_origin),
            ("DunderDictAlias", runtime_v1_origin),
            ("IterAlias", runtime_v1_origin),
            ("ListSubscriptAlias", runtime_v1_origin),
            ("MatchAlias", runtime_v1_origin),
            ("TupleSubscriptAlias", runtime_v1_origin),
            ("UnknownAlias", runtime_v1_origin),
            ("VarsAlias", runtime_v1_origin),
            ("Versioned", runtime_v1_origin),
            ("WalrusAlias", runtime_v1_origin),
        ),
        "analyzer_reflective_call_provenance": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
            ("AliasedDunderMappingAlias", "assignment", runtime_v1_origin),
            ("AliasedGetattrAlias", "assignment", runtime_v1_origin),
            ("AliasedMappingAlias", "assignment", runtime_v1_origin),
            ("AliasedOperatorMappingAlias", "assignment", runtime_v1_origin),
            ("AliasedVarsMappingAlias", "assignment", runtime_v1_origin),
            ("DirectGetattrAlias", "assignment", runtime_v1_origin),
            ("DirectMappingAlias", "assignment", runtime_v1_origin),
            ("DunderMappingItemAlias", "assignment", runtime_v1_origin),
            ("MappingItemAlias", "assignment", runtime_v1_origin),
            ("OpaqueMappingAccessorAlias", "assignment", runtime_v1_origin),
            ("OperatorMappingAlias", "assignment", runtime_v1_origin),
            ("VarsMappingAlias", "assignment", runtime_v1_origin),
            ("VarsMappingItemAlias", "assignment", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
        ),
        "analyzer_decorator_provenance": (
            ("DecoratedClass", "class", runtime_v1_origin),
            ("DecoratedFunction", "function", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
        ),
        "analyzer_decorator_reexports": (
            ("DecoratedClass", runtime_v1_origin),
            ("DecoratedFunction", runtime_v1_origin),
            ("Versioned", runtime_v1_origin),
        ),
        "analyzer_dynamic_module_binding": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", runtime_v1_origin),
            ("Versioned", "import", runtime_v1_origin),
        ),
        "analyzer_dynamic_module_reexport": (
            (_V12_DYNAMIC_MODULE_BINDING, _V12_DYNAMIC_MODULE_BINDING),
            (_V12_DYNAMIC_MODULE_BINDING, runtime_v1_origin),
            ("Versioned", runtime_v1_origin),
        ),
        "analyzer_exec_dynamic_binding": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
        ),
        "analyzer_exec_dynamic_reexport": ((_V12_DYNAMIC_MODULE_BINDING, _V12_DYNAMIC_MODULE_BINDING),),
        "analyzer_hidden_publish_binding": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
        ),
        "analyzer_hidden_publish_reexport": ((_V12_DYNAMIC_MODULE_BINDING, _V12_DYNAMIC_MODULE_BINDING),),
        "analyzer_read_only_mapping_provenance": (
            ("AliasedRead", "assignment", runtime_v1_origin),
            ("DirectRead", "assignment", runtime_v1_origin),
            ("DunderRead", "assignment", runtime_v1_origin),
            ("ItemRead", "assignment", runtime_v1_origin),
            ("OperatorRead", "assignment", runtime_v1_origin),
        ),
        "analyzer_read_only_mapping_has_no_dynamic_binding": True,
        "analyzer_detached_mapping_provenance": (
            (
                "SelectedSensitive",
                "assignment",
                "<reflective-member>.PostgresMssqlSourceSchemaRuntimeV1",
            ),
        ),
        "analyzer_detached_mapping_has_no_dynamic_binding": True,
        "analyzer_safe_bound_value_origins": (
            (
                "copy_reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:copy",
            ),
            (
                "getattr_reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:get",
            ),
            (
                "item_reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:__getitem__",
            ),
            (
                "items_reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:items",
            ),
            (
                "keys_reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:keys",
            ),
            (
                "reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:get",
            ),
            (
                "values_reader",
                "assignment",
                "<bound-safe-accessor>:<current-module-namespace>:values",
            ),
        ),
        "analyzer_safe_bound_values_have_no_dynamic_binding": True,
        "analyzer_typed_namespace_capability_origins": (
            ("module", "assignment", "<current-module-namespace>"),
            ("module", "assignment", "<module-object>:<current-module-namespace>"),
            ("namespace", "assignment", "<module-mapping>:<current-module-namespace>"),
            ("namespace_direct", "assignment", "<module-mapping>:<current-module-namespace>"),
            (
                "setter",
                "assignment",
                "<bound-namespace-mutator>:<current-module-namespace>:__setitem__",
            ),
            (
                "unknown",
                "assignment",
                "<bound-namespace-mutator>:<current-module-namespace>:publish",
            ),
            (
                "update",
                "assignment",
                "<bound-namespace-mutator>:<current-module-namespace>:update",
            ),
        ),
        "analyzer_typed_namespace_mutation_binding": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
        ),
        "analyzer_typed_namespace_mutation_reexport": ((_V12_DYNAMIC_MODULE_BINDING, _V12_DYNAMIC_MODULE_BINDING),),
        "analyzer_module_registry_origins": (
            (
                "mapping_from_attribute",
                "assignment",
                "<module-mapping>:<current-module-namespace>",
            ),
            (
                "mapping_from_getattr",
                "assignment",
                "<module-mapping>:<current-module-namespace>",
            ),
            ("module_via_bound", "assignment", "<current-module-namespace>"),
            (
                "module_via_bound",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("module_via_get", "assignment", "<current-module-namespace>"),
            (
                "module_via_get",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("module_via_operator", "assignment", "<current-module-namespace>"),
            (
                "module_via_operator",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("module_via_subscript", "assignment", "<current-module-namespace>"),
            (
                "module_via_subscript",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("registry", "assignment", "<module-registry>:sys.modules"),
        ),
        "analyzer_module_registry_mutation_binding": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
        ),
        "analyzer_imported_registry_origins": (
            ("module_via_bound", "assignment", "<current-module-namespace>"),
            (
                "module_via_bound",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("module_via_get", "assignment", "<current-module-namespace>"),
            (
                "module_via_get",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("module_via_operator", "assignment", "<current-module-namespace>"),
            (
                "module_via_operator",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("module_via_subscript", "assignment", "<current-module-namespace>"),
            (
                "module_via_subscript",
                "assignment",
                "<module-object>:<current-module-namespace>",
            ),
            ("registry", "import", "<module-registry>:sys.modules"),
        ),
        "analyzer_imported_registry_mutation_binding": (
            (_V12_DYNAMIC_MODULE_BINDING, "dynamic_module_binding", _V12_DYNAMIC_MODULE_BINDING),
        ),
        "analyzer_unrelated_mutation_has_no_dynamic_binding": True,
        "analyzer_function_local_lock_alias": "threading.Lock",
        "terminal_lifecycle_receipt_mutations": (),
    }
    assert observed == literal_golden


def test_v12_boundary_factories_and_exact_consumers_match_literal_static_golden() -> None:
    import ast

    root = Path("src/dpone")
    boundary_path = root / "runtime/sources/strategies/postgres/postgres_prepared_source_boundary.py"
    projection_path = root / "runtime/sources/postgres_mssql_source_schema_projection.py"
    runtime_path = root / "runtime/postgres_mssql_source_schema_runtime.py"
    expected_external_callers = {
        "build_r1_postgres_fetched_schema": (projection_path,),
        "issue_r1_prepared_postgres_source_boundary": (runtime_path,),
    }
    expected_signatures = {
        "build_r1_postgres_fetched_schema": (
            (),
            (
                ("relation_schema", "tuple[tuple[str, str], ...]"),
                ("projected_schema", "tuple[tuple[str, str], ...]"),
                ("relation_metadata", "tuple[SourceColumnProvenance, ...]"),
                ("target_projection", "PostgresMssqlSchemaProjection"),
            ),
            "PostgresFetchedSchema",
        ),
        "issue_r1_prepared_postgres_source_boundary": (
            (),
            (
                ("connector", "object"),
                ("scope", "PostgresVerifiedRelationSnapshotScopeV1"),
                ("source_schema_authority", "PostgresMssqlSelectedRelationSchemaAuthorityV1"),
                ("schema_projection", "PostgresFetchedSchema"),
            ),
            "PreparedPostgresSourceBoundary",
        ),
    }
    all_python = tuple(root.rglob("*.py"))
    boundary_exists = boundary_path.is_file()
    boundary_tree = _v12_parse(boundary_path)
    top_level_functions = {
        node.name: node for node in boundary_tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    observed_signatures: dict[str, object] = {}
    observed_constructor_flow: dict[str, object] = {}
    for name in expected_signatures:
        function = top_level_functions.get(name)
        if function is None:
            observed_signatures[name] = None
            continue
        observed_signatures[name] = (
            tuple(argument.arg for argument in (*function.args.posonlyargs, *function.args.args)),
            tuple(
                (argument.arg, ast.unparse(argument.annotation) if argument.annotation is not None else "")
                for argument in function.args.kwonlyargs
            ),
            ast.unparse(function.returns) if function.returns is not None else "",
        )
        declared_return = expected_signatures[name][2]
        observed_constructor_flow[name] = _v12_factory_returns_own_constructor(function, declared_return)

    production_trees = tuple((path, _v12_parse(path)) for path in all_python)
    observed_external_callers, reexports, dynamic_loaders = _v12_symbol_graph(
        production_trees,
        tuple(expected_signatures),
        boundary_path=boundary_path,
    )
    good_factory = ast.parse("def factory():\n    result = Declared()\n    alias = result\n    return alias\n").body[0]
    dead_constructor = ast.parse(
        "def factory():\n    if False:\n        result = Declared()\n    return delegated()\n"
    ).body[0]
    mixed_returns = ast.parse(
        "def factory(flag):\n    result = Declared()\n    if flag:\n        return result\n    return delegated()\n"
    ).body[0]
    branch_overwrite = ast.parse(
        "def factory(flag):\n    result = delegated()\n    if flag:\n        result = Declared()\n    return result\n"
    ).body[0]
    for_return = ast.parse(
        "def factory(items):\n"
        "    result = Declared()\n"
        "    for item in items:\n"
        "        return delegated()\n"
        "    return result\n"
    ).body[0]
    while_return = ast.parse(
        "def factory(flag):\n    result = Declared()\n    while flag:\n        return delegated()\n    return result\n"
    ).body[0]
    with_return = ast.parse(
        "def factory(context):\n"
        "    result = Declared()\n"
        "    with context:\n"
        "        return delegated()\n"
        "    return result\n"
    ).body[0]
    match_return = ast.parse(
        "def factory(value):\n"
        "    result = Declared()\n"
        "    match value:\n"
        "        case 1:\n"
        "            return delegated()\n"
        "        case _:\n"
        "            return result\n"
    ).body[0]
    async_suite_return = ast.parse(
        "async def factory(items, context):\n"
        "    result = Declared()\n"
        "    async for item in items:\n"
        "        return delegated()\n"
        "    async with context:\n"
        "        return result\n"
    ).body[0]
    try_star_return = ast.parse(
        "def factory():\n"
        "    result = Declared()\n"
        "    try:\n"
        "        raise Exception()\n"
        "    except* Exception:\n"
        "        return delegated()\n"
        "    return result\n"
    ).body[0]
    nested_return = ast.parse(
        "def factory():\n"
        "    def delegated_factory():\n"
        "        return delegated()\n"
        "    callback = lambda: delegated()\n"
        "    return Declared()\n"
    ).body[0]
    rebound_by_def = ast.parse(
        "def factory():\n    result = Declared()\n    def result():\n        return delegated()\n    return result\n"
    ).body[0]
    rebound_by_async_def = ast.parse(
        "def factory():\n"
        "    result = Declared()\n"
        "    async def result():\n"
        "        return delegated()\n"
        "    return result\n"
    ).body[0]
    rebound_by_class = ast.parse(
        "def factory():\n    result = Declared()\n    class result:\n        pass\n    return result\n"
    ).body[0]
    rebound_by_import = ast.parse(
        "def factory():\n    result = Declared()\n    import json as result\n    return result\n"
    ).body[0]
    rebound_by_import_from = ast.parse(
        "def factory():\n    result = Declared()\n    from json import loads as result\n    return result\n"
    ).body[0]
    walrus_if = ast.parse(
        "def factory():\n    result = Declared()\n    if (result := delegated()):\n        pass\n    return result\n"
    ).body[0]
    walrus_while = ast.parse(
        "def factory():\n    result = Declared()\n    while (result := delegated()):\n        pass\n    return result\n"
    ).body[0]
    walrus_match = ast.parse(
        "def factory():\n"
        "    result = Declared()\n"
        "    match (result := delegated()):\n"
        "        case _:\n"
        "            pass\n"
        "    return result\n"
    ).body[0]
    walrus_with = ast.parse(
        "def factory(context):\n"
        "    result = Declared()\n"
        "    with (result := context):\n"
        "        pass\n"
        "    return result\n"
    ).body[0]
    local_alias_path = Path("synthetic/local_alias.py")
    nonliteral_export_path = Path("synthetic/nonliteral_export.py")
    reexport_path = Path("synthetic/reexport.py")
    star_reexport_path = Path("synthetic/star_reexport.py")
    dynamic_path = Path("synthetic/dynamic.py")
    synthetic_graph = _v12_symbol_graph(
        (
            (
                local_alias_path,
                ast.parse(
                    "def consume(enabled):\n"
                    "    if enabled:\n"
                    f"        from {_V12_BOUNDARY_MODULE} import build_r1_postgres_fetched_schema as imported\n"
                    "        factory = imported\n"
                    "        return factory()\n"
                ),
            ),
            (
                nonliteral_export_path,
                ast.parse(
                    f"from {_V12_BOUNDARY_MODULE} import build_r1_postgres_fetched_schema as exported\n"
                    "__all__ = make_exports()\n"
                ),
            ),
            (
                reexport_path,
                ast.parse(
                    f"from {_V12_BOUNDARY_MODULE} import issue_r1_prepared_postgres_source_boundary as exported\n"
                ),
            ),
            (
                star_reexport_path,
                ast.parse(f"from {_V12_BOUNDARY_MODULE} import *\n"),
            ),
            (
                dynamic_path,
                ast.parse(
                    "import importlib as imports\n"
                    "loader = imports.import_module\n"
                    "builtin_loader = __import__\n"
                    "attribute = getattr\n"
                    f"MODULE = '{_V12_BOUNDARY_MODULE}'\n"
                    "MODULE_ALIAS = MODULE\n"
                    "SYMBOL = 'build_r1_postgres_fetched_schema'\n"
                    "module = loader(MODULE_ALIAS)\n"
                    "module2 = builtin_loader(MODULE)\n"
                    "factory = attribute(module, SYMBOL)\n"
                ),
            ),
        ),
        tuple(expected_signatures),
        boundary_path=boundary_path,
    )
    forbidden_port = root / "ports/postgres_mssql_source_schema_runtime.py"
    forbidden_capability_definitions = {
        node.name for node in ast.walk(boundary_tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    } & {"_from_runtime_issued", "_build_fetched_schema", "issue_prepared_boundary"}
    observed = {
        "boundary_exists": boundary_exists,
        "signatures": observed_signatures,
        "constructor_flow": observed_constructor_flow,
        "external_callers": observed_external_callers,
        "reexports": reexports,
        "dynamic_loaders": dynamic_loaders,
        "forbidden_capability_definitions": forbidden_capability_definitions,
        "internal_pass_through_callers": _v12_internal_factory_callers(boundary_tree, tuple(expected_signatures)),
        "forbidden_port_absent": not forbidden_port.exists(),
        "analyzer_accepts_traced_constructor_alias": _v12_factory_returns_own_constructor(good_factory, "Declared"),
        "analyzer_rejects_dead_constructor_delegation": _v12_factory_returns_own_constructor(
            dead_constructor, "Declared"
        ),
        "analyzer_rejects_one_delegated_return": _v12_factory_returns_own_constructor(mixed_returns, "Declared"),
        "analyzer_rejects_branch_only_constructor": _v12_factory_returns_own_constructor(branch_overwrite, "Declared"),
        "analyzer_rejects_for_return": _v12_factory_returns_own_constructor(for_return, "Declared"),
        "analyzer_rejects_while_return": _v12_factory_returns_own_constructor(while_return, "Declared"),
        "analyzer_rejects_with_return": _v12_factory_returns_own_constructor(with_return, "Declared"),
        "analyzer_rejects_match_return": _v12_factory_returns_own_constructor(match_return, "Declared"),
        "analyzer_rejects_async_suite_return": _v12_factory_returns_own_constructor(async_suite_return, "Declared"),
        "analyzer_rejects_try_star_return": _v12_factory_returns_own_constructor(try_star_return, "Declared"),
        "analyzer_excludes_nested_callable_returns": _v12_factory_returns_own_constructor(nested_return, "Declared"),
        "analyzer_rejects_def_rebinding": _v12_factory_returns_own_constructor(rebound_by_def, "Declared"),
        "analyzer_rejects_async_def_rebinding": _v12_factory_returns_own_constructor(rebound_by_async_def, "Declared"),
        "analyzer_rejects_class_rebinding": _v12_factory_returns_own_constructor(rebound_by_class, "Declared"),
        "analyzer_rejects_import_rebinding": _v12_factory_returns_own_constructor(rebound_by_import, "Declared"),
        "analyzer_rejects_import_from_rebinding": _v12_factory_returns_own_constructor(
            rebound_by_import_from, "Declared"
        ),
        "analyzer_rejects_walrus_if_rebinding": _v12_factory_returns_own_constructor(walrus_if, "Declared"),
        "analyzer_rejects_walrus_while_rebinding": _v12_factory_returns_own_constructor(walrus_while, "Declared"),
        "analyzer_rejects_walrus_match_rebinding": _v12_factory_returns_own_constructor(walrus_match, "Declared"),
        "analyzer_rejects_walrus_with_rebinding": _v12_factory_returns_own_constructor(walrus_with, "Declared"),
        "analyzer_finds_function_local_import_alias_call": synthetic_graph[0]["build_r1_postgres_fetched_schema"],
        "analyzer_finds_reexports": synthetic_graph[1],
        "analyzer_finds_dynamic_aliases": tuple(kind for _path, kind in synthetic_graph[2]),
    }
    literal_golden = {
        "boundary_exists": True,
        "signatures": expected_signatures,
        "constructor_flow": {
            "build_r1_postgres_fetched_schema": (1, True),
            "issue_r1_prepared_postgres_source_boundary": (1, True),
        },
        "external_callers": expected_external_callers,
        "reexports": (),
        "dynamic_loaders": (),
        "forbidden_capability_definitions": set(),
        "internal_pass_through_callers": (),
        "forbidden_port_absent": True,
        "analyzer_accepts_traced_constructor_alias": (1, True),
        "analyzer_rejects_dead_constructor_delegation": (0, False),
        "analyzer_rejects_one_delegated_return": (1, False),
        "analyzer_rejects_branch_only_constructor": (1, False),
        "analyzer_rejects_for_return": (1, False),
        "analyzer_rejects_while_return": (1, False),
        "analyzer_rejects_with_return": (1, False),
        "analyzer_rejects_match_return": (1, False),
        "analyzer_rejects_async_suite_return": (1, False),
        "analyzer_rejects_try_star_return": (1, False),
        "analyzer_excludes_nested_callable_returns": (1, True),
        "analyzer_rejects_def_rebinding": (1, False),
        "analyzer_rejects_async_def_rebinding": (1, False),
        "analyzer_rejects_class_rebinding": (1, False),
        "analyzer_rejects_import_rebinding": (1, False),
        "analyzer_rejects_import_from_rebinding": (1, False),
        "analyzer_rejects_walrus_if_rebinding": (1, False),
        "analyzer_rejects_walrus_while_rebinding": (1, False),
        "analyzer_rejects_walrus_match_rebinding": (1, False),
        "analyzer_rejects_walrus_with_rebinding": (1, False),
        "analyzer_finds_function_local_import_alias_call": (local_alias_path,),
        "analyzer_finds_reexports": (
            (dynamic_path, "build_r1_postgres_fetched_schema"),
            (nonliteral_export_path, "<nonliteral-__all__>"),
            (reexport_path, "issue_r1_prepared_postgres_source_boundary"),
            (star_reexport_path, "*"),
        ),
        "analyzer_finds_dynamic_aliases": (
            "__import__",
            "getattr",
            "importlib.import_module",
        ),
    }
    assert observed == literal_golden
