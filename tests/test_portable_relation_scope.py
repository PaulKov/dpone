"""Focused contract tests for governed PostgreSQL→MSSQL portable scopes."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, tzinfo
from decimal import Decimal
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import (
    MSSQLStrategyContractError,
    normalize_mssql_load_strategy,
)
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.mssql_transaction_governance import InvocationIdentity, MssqlTransactionContractError
from dpone.contracts.portable_relation_scope import (
    PortableEqualityScope,
    PortableLiteral,
    PortableRangeScope,
    PortableScopeContractError,
    parse_portable_relation_scope,
    portable_scope_canonical_json,
    portable_scope_sha256,
)
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeBindingError,
    PortableScopeColumnContract,
    bind_portable_scope,
    require_exact_portable_identifier,
)
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
from dpone.runtime.etl.mssql_transaction_identity import (
    invocation_route_fingerprint,
    operation_request,
)
from dpone.runtime.etl.portable_scope_preflight import (
    prepare_portable_scope_binding,
    resolve_portable_scope_column_contract,
)
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlSchemaCatalogSnapshot,
    catalog_column_from_definition,
)
from dpone.runtime.sinks.mssql_transaction_requirement import MSSQL_GENERIC_TRANSACTION_CAPABILITY
from dpone.runtime.sinks.strategies.mssql.mssql_portable_scope import render_mssql_portable_scope
from dpone.runtime.sources.strategies.postgres.postgres_base_strategy import PostgresFetchedSchema
from dpone.runtime.sources.strategies.postgres.postgres_portable_scope import (
    render_postgres_portable_scope,
)
from dpone.runtime.state.mssql_generic_transaction_storage import (
    MssqlGenericTransactionStateStorage,
)
from dpone.runtime.support.postgres_mssql_projection import (
    PostgresMssqlColumnProjection,
    PostgresMssqlSchemaProjection,
)

_SOURCE_AUTHORITY_SHA256 = "sha256:" + "a" * 64


def test_integer_equality_ast_has_stable_sql_free_digest_and_exact_unicode_identifier() -> None:
    scope = parse_portable_relation_scope(
        {
            "version": 1,
            "kind": "equality",
            "column": "tenant.ключ",
            "value": {"type": "integer", "value": 7},
        }
    )

    assert scope.column == "tenant.ключ"
    assert portable_scope_canonical_json(scope) == (
        '{"column":"tenant.ключ","kind":"equality","value":{"type":"integer","value":7},"version":1}'
    )
    assert len(portable_scope_sha256(scope)) == 32
    assert "SELECT" not in portable_scope_canonical_json(scope)


@pytest.mark.parametrize(
    ("column", "blocker"),
    (
        ("", "portable_scope.column_empty"),
        ("bad\x00name", "portable_scope.column_nul"),
        ("я" * 32, "portable_scope.column_length"),
    ),
)
def test_identifier_portability_rejects_only_empty_nul_and_over_byte_limit(
    column: str,
    blocker: str,
) -> None:
    with pytest.raises(PortableScopeContractError, match=blocker):
        _scope(column=column)


def test_range_requires_one_exact_literal_family_and_rejects_text_ranges() -> None:
    with pytest.raises(PortableScopeContractError, match="portable_scope.range.type_mismatch"):
        parse_portable_relation_scope(
            {
                "kind": "range",
                "column": "amount",
                "lower": {"value": {"type": "integer", "value": 1}},
                "upper": {"value": {"type": "decimal", "value": "2.5"}},
            }
        )
    with pytest.raises(PortableScopeContractError, match="portable_scope.range.literal_type"):
        parse_portable_relation_scope(
            {
                "kind": "range",
                "column": "label",
                "lower": {"value": {"type": "text", "value": "a"}},
            }
        )


def test_integer_scope_binding_and_both_renderers_use_parameters_and_one_quoted_identifier() -> None:
    injection = 'tenant.id" = 1 OR "x'
    scope = _scope(column=injection, value=17)
    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract(
            source_name=injection,
            source_type="integer",
            source_collation=None,
            target_name=injection,
            target_type="int",
            target_collation=None,
        ),
    )

    postgres = render_postgres_portable_scope(scope, binding)
    mssql = render_mssql_portable_scope(scope, binding, quote_identifier=_mssql_quote)

    assert postgres.params == (17,)
    assert mssql.sql == '[tenant.id" = 1 OR "x] = ?'
    assert mssql.params == (17,)
    assert "17" not in mssql.sql
    postgres_sql = getattr(postgres.sql, "as_string", None)
    if callable(postgres_sql):
        rendered_postgres = postgres_sql(None)
        assert rendered_postgres == '"tenant.id"" = 1 OR ""x" = %s'
        assert "17" not in rendered_postgres


def test_uuid_range_uses_postgres_uuid_index_semantics_and_canonical_mssql_order() -> None:
    scope = parse_portable_relation_scope(
        {
            "kind": "range",
            "column": "id",
            "lower": {
                "inclusive": True,
                "value": {"type": "uuid", "value": "00000000-0000-0000-0000-000000000000"},
            },
            "upper": {
                "inclusive": False,
                "value": {"type": "uuid", "value": "04000000-0000-0000-0000-000000000000"},
            },
        }
    )
    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract("id", "uuid", None, "id", "uniqueidentifier", None),
    )

    postgres = render_postgres_portable_scope(scope, binding)
    mssql = render_mssql_portable_scope(scope, binding, quote_identifier=_mssql_quote)

    assert (
        postgres.params
        == mssql.params
        == (
            "00000000-0000-0000-0000-000000000000",
            "04000000-0000-0000-0000-000000000000",
        )
    )
    postgres_sql = getattr(postgres.sql, "as_string", None)
    if callable(postgres_sql):
        assert postgres_sql(None) == '"id" >= %s::uuid AND "id" < %s::uuid'
    assert mssql.sql == (
        "LOWER(CONVERT(char(36), [id])) COLLATE Latin1_General_100_BIN2 >= "
        "LOWER(CONVERT(char(36), ?)) COLLATE Latin1_General_100_BIN2 AND "
        "LOWER(CONVERT(char(36), [id])) COLLATE Latin1_General_100_BIN2 < "
        "LOWER(CONVERT(char(36), ?)) COLLATE Latin1_General_100_BIN2"
    )
    assert binding.comparison_contract == "ordered_canonical_uuid_v1"


def test_uuid_literal_requires_canonical_lowercase_hyphenated_form() -> None:
    with pytest.raises(PortableScopeContractError, match="uuid_canonical"):
        parse_portable_relation_scope(
            {
                "kind": "equality",
                "column": "id",
                "value": {"type": "uuid", "value": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"},
            }
        )


def test_decimal_precision_and_integer_range_must_fit_both_catalog_types() -> None:
    decimal_scope = parse_portable_relation_scope(
        {
            "kind": "equality",
            "column": "amount",
            "value": {"type": "decimal", "value": "123.456"},
        }
    )
    with pytest.raises(PortableScopeBindingError, match="portable_scope.binding.decimal_precision"):
        bind_portable_scope(
            decimal_scope,
            PortableScopeColumnContract("amount", "numeric(8,3)", None, "amount", "decimal(5,2)", None),
        )

    integer_scope = _scope(column="partition_id", value=40_000)
    with pytest.raises(PortableScopeBindingError, match="portable_scope.binding.integer_range"):
        bind_portable_scope(
            integer_scope,
            PortableScopeColumnContract("partition_id", "integer", None, "partition_id", "smallint", None),
        )


def test_text_equality_requires_certified_binary_collations_and_renderers_force_binary_comparison() -> None:
    scope = parse_portable_relation_scope(
        {"kind": "equality", "column": "label", "value": {"type": "text", "value": "βeta "}}
    )
    with pytest.raises(PortableScopeBindingError, match="portable_scope.binding.text_collation"):
        bind_portable_scope(
            scope,
            PortableScopeColumnContract(
                "label",
                "text",
                "pg_catalog.default",
                "label",
                "nvarchar(max)",
                "Latin1_General_100_CI_AS",
            ),
        )

    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract(
            "label",
            "text",
            "pg_catalog.C",
            "label",
            "nvarchar(max)",
            "Latin1_General_100_BIN2",
        ),
    )
    postgres = render_postgres_portable_scope(scope, binding)
    mssql = render_mssql_portable_scope(scope, binding, quote_identifier=_mssql_quote)
    assert "CONVERT(varbinary(max), [label])" in mssql.sql
    assert postgres.params == mssql.params == ("βeta ",)
    postgres_sql = getattr(postgres.sql, "as_string", None)
    if callable(postgres_sql):
        assert "convert_to(\"label\"::text, 'UTF8')" in postgres_sql(None)


@pytest.mark.parametrize(
    ("requested", "candidates", "blocker"),
    (
        ("id", ("id", "ID"), "source_identifier_ambiguous"),
        ("id", ("ID",), "source_identifier_case_mismatch"),
        ("id", ("other",), "source_identifier_unknown"),
    ),
)
def test_catalog_identifier_binding_fails_closed_on_ambiguity_case_and_unknown(
    requested: str,
    candidates: tuple[str, ...],
    blocker: str,
) -> None:
    with pytest.raises(PortableScopeBindingError, match=blocker):
        require_exact_portable_identifier(requested, candidates, side="source")


def test_scope_binding_rejects_target_rename_and_cross_type_comparison() -> None:
    scope = _scope()
    with pytest.raises(PortableScopeBindingError, match="portable_scope.binding.renamed_target"):
        bind_portable_scope(
            scope,
            PortableScopeColumnContract("partition_id", "integer", None, "partition_id_v2", "int", None),
        )
    with pytest.raises(PortableScopeBindingError, match="portable_scope.binding.type_mismatch"):
        bind_portable_scope(
            scope,
            PortableScopeColumnContract(
                "partition_id",
                "integer",
                None,
                "partition_id",
                "nvarchar(20)",
                "Latin1_General_100_BIN2",
            ),
        )


def test_strategy_keeps_same_dialect_raw_legacy_but_requires_typed_cross_dialect_scope() -> None:
    portable = _config(portable_scope=_scope())
    policy = normalize_mssql_load_strategy(portable).replace
    assert policy is not None
    assert policy.scope_kind == "portable_relation_scope_v1"
    assert policy.portable_scope_sha256 == portable_scope_sha256(_scope()).hex()

    raw_cross = _config(custom_predicate="partition_id = 1")
    with pytest.raises(MSSQLStrategyContractError, match="cross_dialect_raw_predicate"):
        normalize_mssql_load_strategy(raw_cross)

    raw_same = replace(
        raw_cross,
        options={**raw_cross.options, "source_type": "mssql"},
    )
    raw_policy = normalize_mssql_load_strategy(raw_same).replace
    assert raw_policy is not None and raw_policy.scope_kind == "legacy_raw_same_dialect"


def test_builder_parses_sink_strategy_portable_scope_into_the_typed_ast() -> None:
    load_config = LoadConfigBuilder().build(
        {
            "name": "portable_replace",
            "source": {
                "type": "postgres",
                "connection_id": "source",
                "table": {"schema": "public", "name": "events"},
            },
            "sink": {
                "type": "mssql",
                "connection_id": "target",
                "table": {"database": "target_db", "schema": "dbo", "name": "events"},
                "strategy": {
                    "mode": "replace",
                    "portable_scope": {
                        "kind": "equality",
                        "column": "partition_id",
                        "value": {"type": "integer", "value": 1},
                    },
                },
            },
        }
    )

    assert load_config.portable_scope == _scope()
    assert load_config.custom_predicate is None


def test_route_and_operation_identities_hash_ast_and_binding_not_rendered_sql() -> None:
    first = _bound_config(value=1)
    second = _bound_config(value=2)
    source_identity = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="cluster",
        database="source_db",
        effective_principal="etl",
        session_principal="etl",
        topology_role="primary",
    )
    target_identity = b"t" * 32
    invocation = InvocationIdentity("run", "process", "task")

    assert invocation_route_fingerprint(
        first,
        target_identity=target_identity,
        source_identity=source_identity,
    ) != invocation_route_fingerprint(
        second,
        target_identity=target_identity,
        source_identity=source_identity,
    )
    assert operation_request(first, invocation).scope_hash != operation_request(second, invocation).scope_hash


def test_ambiguous_source_catalog_fails_after_authority_preflight_before_target_catalog_or_admit() -> None:
    scope = _scope()
    columns = (
        _projection_column("partition_id"),
        _projection_column("PARTITION_ID"),
    )
    fetched = PostgresFetchedSchema(
        relation_schema=(("partition_id", "integer"), ("PARTITION_ID", "integer")),
        projected_schema=(("partition_id", "int"), ("PARTITION_ID", "int")),
        relation_metadata=(),
        target_projection=PostgresMssqlSchemaProjection(columns),
    )
    source = SimpleNamespace(
        fetch_schema_projection=lambda _config: fetched,
        mssql_transaction_checkpoint_mode=lambda _config: MssqlTransactionCheckpointMode.STATELESS,
        mssql_transaction_source_physical_identity=lambda _config: _source_identity_v2(),
    )
    catalog_calls: list[object] = []
    state_storage = SimpleNamespace(
        atomicity="target_atomic",
        provisioning="external",
        require_database_authority_binding=lambda: None,
    )
    sink = SimpleNamespace(
        connector=object(),
        state_storage=state_storage,
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
        get_target_catalog_snapshot=lambda config: catalog_calls.append(config),
    )
    state_preflights: list[object] = []
    state = SimpleNamespace(
        preflight=lambda connector: state_preflights.append(connector),
        admit=lambda *_args: pytest.fail("state admission must not run"),
    )
    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *_args, **_kwargs: SimpleNamespace(
            digest=b"t" * 32,
            database_name="target_db",
            schema_name="dbo",
            table_name="events",
        ),
        state_factory=lambda storage: state if storage is state_storage else pytest.fail("wrong state"),
    )

    with pytest.raises(PortableScopeBindingError, match="source_identifier_ambiguous"):
        service.prepare(
            _config(portable_scope=scope),
            source=source,
            sink=sink,
            run_context=SimpleNamespace(),
            load_record=SimpleNamespace(),
            dag_id="portable_scope",
        )
    assert state_preflights == []
    assert catalog_calls == []


def test_unbound_portable_route_fails_before_source_or_target_catalog_io() -> None:
    catalog_calls = {"source": 0, "target": 0}

    def source_catalog(_config: LoadConfig) -> object:
        catalog_calls["source"] += 1
        raise AssertionError("source catalog must not run before signed database authority")

    def target_catalog(_config: LoadConfig) -> object:
        catalog_calls["target"] += 1
        raise AssertionError("target catalog must not run before signed database authority")

    source = SimpleNamespace(
        fetch_schema_projection=source_catalog,
        mssql_transaction_checkpoint_mode=lambda _config: MssqlTransactionCheckpointMode.STATELESS,
    )
    storage = MssqlGenericTransactionStateStorage(
        object(),
        database="state_db",
        schema="system",
    )
    sink = SimpleNamespace(
        connector=object(),
        state_storage=storage,
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
        get_target_catalog_snapshot=target_catalog,
    )

    with pytest.raises(RuntimeError, match="database_authority_verifier_required"):
        MssqlTransactionAdmissionService().prepare(
            _config(portable_scope=_scope()),
            source=source,
            sink=sink,
            run_context=SimpleNamespace(),
            load_record=SimpleNamespace(),
            dag_id="portable_scope",
        )

    assert catalog_calls == {"source": 0, "target": 0}


def test_missing_signed_source_binding_fails_before_live_identity_catalog_or_state_io() -> None:
    calls = {"target_identity": 0, "source_identity": 0, "source_catalog": 0, "state": 0}
    config = _config(portable_scope=_scope())
    config.options.pop("postgres_source_authority_sha256")

    def target_identity(*_args: object, **_kwargs: object) -> object:
        calls["target_identity"] += 1
        raise AssertionError("target identity must not run before signed source binding completeness")

    source = SimpleNamespace(
        mssql_transaction_checkpoint_mode=lambda _config: MssqlTransactionCheckpointMode.STATELESS,
        mssql_transaction_source_physical_identity=lambda _config: calls.__setitem__(
            "source_identity", calls["source_identity"] + 1
        ),
        fetch_schema_projection=lambda _config: calls.__setitem__("source_catalog", calls["source_catalog"] + 1),
    )
    state_storage = SimpleNamespace(
        atomicity="target_atomic",
        provisioning="external",
        require_database_authority_binding=lambda: None,
    )
    sink = SimpleNamespace(
        connector=object(),
        state_storage=state_storage,
        target_dialect=lambda: "mssql",
        mssql_transaction_governance_capability=lambda: MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    )
    service = MssqlTransactionAdmissionService(
        target_resolver=target_identity,
        state_factory=lambda _storage: calls.__setitem__("state", calls["state"] + 1),
    )

    with pytest.raises(MssqlTransactionContractError, match="postgres_source_authority_digest_required"):
        service.prepare(
            config,
            source=source,
            sink=sink,
            run_context=SimpleNamespace(),
            load_record=SimpleNamespace(),
            dag_id="portable_scope",
        )

    assert calls == {"target_identity": 0, "source_identity": 0, "source_catalog": 0, "state": 0}


def test_existing_target_widening_binds_identity_to_deterministic_expected_after_shape() -> None:
    scope = _scope()
    projection = PostgresMssqlSchemaProjection(
        (_projection_column("partition_id", source_type="bigint", target_type="bigint"),)
    )
    fetched = PostgresFetchedSchema(
        relation_schema=(("partition_id", "bigint"),),
        projected_schema=(("partition_id", "bigint"),),
        relation_metadata=(),
        target_projection=projection,
    )
    source = SimpleNamespace(fetch_schema_projection=lambda _config: fetched)
    observed = {"snapshot": _target_snapshot("int")}
    sink = SimpleNamespace(get_target_catalog_snapshot=lambda _config: observed["snapshot"])

    before = prepare_portable_scope_binding(
        _config(portable_scope=scope),
        source=source,
        sink=sink,
    )
    observed["snapshot"] = _target_snapshot("bigint")
    after = prepare_portable_scope_binding(
        _config(portable_scope=scope),
        source=source,
        sink=sink,
    )
    before_binding = before.options[PORTABLE_SCOPE_BINDING_OPTION]
    after_binding = after.options[PORTABLE_SCOPE_BINDING_OPTION]

    assert before_binding == after_binding
    assert before_binding.target_type == "bigint"
    assert before_binding.comparison_contract == "exact_integer_v1"
    source_identity = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="cluster",
        database="source_db",
        effective_principal="etl",
        session_principal="etl",
        topology_role="primary",
    )
    assert invocation_route_fingerprint(
        before,
        target_identity=b"t" * 32,
        source_identity=source_identity,
    ) == invocation_route_fingerprint(
        after,
        target_identity=b"t" * 32,
        source_identity=source_identity,
    )


def test_parent_issued_exact_binding_skips_child_catalog_preflight() -> None:
    scope = _scope()
    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract("partition_id", "bigint", None, "partition_id", "bigint", None),
    )
    config = _config(portable_scope=scope)
    config = replace(
        config,
        options={**config.options, PORTABLE_SCOPE_BINDING_OPTION: binding},
    )
    source = SimpleNamespace(
        fetch_schema_projection=lambda _config: pytest.fail("child must not query PostgreSQL catalog")
    )
    sink = SimpleNamespace(
        get_target_catalog_snapshot=lambda _config: pytest.fail("child must not query MSSQL catalog")
    )

    prepared = prepare_portable_scope_binding(config, source=source, sink=sink)

    assert prepared.options[PORTABLE_SCOPE_BINDING_OPTION] == binding


def test_parent_resolves_backfill_column_contract_before_planner_issues_scope() -> None:
    """Campaign catalog proof must not impersonate scoped chunk admission."""

    projection = PostgresMssqlSchemaProjection(
        (_projection_column("partition_id", source_type="bigint", target_type="bigint"),)
    )
    fetched = PostgresFetchedSchema(
        relation_schema=(("partition_id", "bigint"),),
        projected_schema=(("partition_id", "bigint"),),
        relation_metadata=(),
        target_projection=projection,
    )
    source = SimpleNamespace(fetch_schema_projection=lambda _config: fetched)
    sink = SimpleNamespace(get_target_catalog_snapshot=lambda _config: _target_snapshot("bigint"))
    options = {
        **_config().options,
        "backfill": {
            "inner_mode": "replace",
            "parallel_workers": 1,
            "chunk": {
                "column": "partition_id",
                "kind": "integer",
                "from": "1",
                "to": "2",
                "step": "1",
            },
        },
    }
    campaign = replace(_config(), load_strategy=LoadStrategy.BACKFILL, options=options)

    with pytest.raises(MSSQLStrategyContractError, match="scope_required"):
        normalize_mssql_load_strategy(campaign)

    assert resolve_portable_scope_column_contract(
        campaign,
        column="partition_id",
        source=source,
        sink=sink,
    ) == PortableScopeColumnContract(
        "partition_id",
        "bigint",
        None,
        "partition_id",
        "bigint",
        None,
    )


def _scope(*, column: str = "partition_id", value: int = 1):
    return parse_portable_relation_scope(
        {"kind": "equality", "column": column, "value": {"type": "integer", "value": value}}
    )


def _config(*, portable_scope=None, custom_predicate: str | None = None) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_database="target_db",
        target_schema="dbo",
        target_table="events",
        load_strategy=LoadStrategy.REPLACE,
        custom_predicate=custom_predicate,
        portable_scope=portable_scope,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "postgres_source_authority_sha256": _SOURCE_AUTHORITY_SHA256,
        },
    )


def _source_identity_v2() -> SourcePhysicalIdentity:
    return SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="cluster",
        database="source_db",
        effective_principal="etl",
        session_principal="etl",
        topology_role="primary",
        version=2,
        authority_sha256=_SOURCE_AUTHORITY_SHA256,
        timeline_id=1,
        database_oid=10,
        effective_principal_oid=11,
        session_principal_oid=11,
        schema="public",
        schema_oid=12,
        relation="events",
        relation_oid=13,
    )


def _bound_config(*, value: int) -> LoadConfig:
    scope = _scope(value=value)
    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract("partition_id", "integer", None, "partition_id", "int", None),
    )
    return replace(
        _config(portable_scope=scope),
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            PORTABLE_SCOPE_BINDING_OPTION: binding,
        },
    )


def _projection_column(
    name: str,
    *,
    source_type: str = "integer",
    target_type: str = "int",
) -> PostgresMssqlColumnProjection:
    return PostgresMssqlColumnProjection(
        name=name,
        source_name=name,
        wire_position=0,
        source_type=source_type,
        source_native_mssql_type=target_type,
        projected_type=target_type,
        target_type=target_type,
        transfer_representation="integer",
        requires_explicit_contract=False,
        explicit_contract_source=None,
        nullable=False,
        source_collation=None,
        collation=None,
    )


def _target_snapshot(dtype: str) -> MssqlSchemaCatalogSnapshot:
    return MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        columns=(
            catalog_column_from_definition(
                ColumnDef("partition_id", dtype, nullable=False),
                ordinal=1,
                database_collation="Latin1_General_100_BIN2",
            ),
        ),
    )


def _mssql_quote(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


@pytest.mark.parametrize("value", ("-0", "1.0", "1.00", "1.2300"))
def test_decimal_authoring_rejects_noncanonical_lexemes(value: str) -> None:
    with pytest.raises(PortableScopeContractError, match="decimal_canonical"):
        parse_portable_relation_scope(
            {"kind": "equality", "column": "amount", "value": {"type": "decimal", "value": value}}
        )


def test_decimal_canonical_identity_preserves_one_exact_lexeme() -> None:
    scope = parse_portable_relation_scope(
        {"kind": "equality", "column": "amount", "value": {"type": "decimal", "value": "1.23"}}
    )
    assert scope.value.value == Decimal("1.23")
    assert '"value":"1.23"' in portable_scope_canonical_json(scope)


def test_direct_ast_construction_cannot_bypass_identifier_literal_or_range_invariants() -> None:
    with pytest.raises(PortableScopeContractError, match="column_empty"):
        PortableEqualityScope("", PortableLiteral("integer", 1))
    with pytest.raises(PortableScopeContractError, match="literal.integer"):
        PortableLiteral("integer", True)
    with pytest.raises(PortableScopeContractError, match="decimal_canonical"):
        PortableLiteral("decimal", Decimal("1.00"))
    with pytest.raises(PortableScopeContractError, match="range.bounds"):
        PortableRangeScope("id", None, None)
    with pytest.raises(PortableScopeContractError, match="portable_scope.kind"):
        PortableEqualityScope("id", PortableLiteral("integer", 1), kind="range")  # type: ignore[arg-type]


def test_timestamptz_canonical_identity_normalizes_midnight_fraction_and_offset() -> None:
    offset = parse_portable_relation_scope(
        {
            "kind": "equality",
            "column": "occurred_at",
            "value": {"type": "timestamptz", "value": "2026-08-16T03:00:00+03:00"},
        }
    )
    utc = parse_portable_relation_scope(
        {
            "kind": "equality",
            "column": "occurred_at",
            "value": {"type": "timestamptz", "value": "2026-08-16T00:00:00Z"},
        }
    )
    fraction = parse_portable_relation_scope(
        {
            "kind": "equality",
            "column": "occurred_at",
            "value": {"type": "timestamptz", "value": "2026-08-16T00:00:00.120000Z"},
        }
    )

    assert portable_scope_sha256(offset) == portable_scope_sha256(utc)
    assert '"value":"2026-08-16T00:00:00Z"' in portable_scope_canonical_json(utc)
    assert '"value":"2026-08-16T00:00:00.12Z"' in portable_scope_canonical_json(fraction)


@pytest.mark.parametrize(
    "value",
    ("0001-01-01T00:00:00+14:00", "9999-12-31T23:59:59-14:00"),
)
def test_timestamptz_offset_that_overflows_sql_server_utc_range_fails_typed(value: str) -> None:
    with pytest.raises(PortableScopeContractError, match="timestamptz_range"):
        parse_portable_relation_scope(
            {
                "kind": "equality",
                "column": "occurred_at",
                "value": {"type": "timestamptz", "value": value},
            }
        )


@pytest.mark.parametrize(
    ("literal_type", "literal", "source_type", "target_type", "expected"),
    (
        ("date", "2026-08-16", "date", "date", "CONVERT(date, ?)"),
        (
            "timestamp",
            "2026-08-16T00:00:00.123456",
            "timestamp(6) without time zone",
            "datetime2(6)",
            "CONVERT(datetime2(6), ?)",
        ),
        (
            "timestamptz",
            "2026-08-16T00:00:00Z",
            "timestamp(6) with time zone",
            "datetimeoffset(6)",
            "CONVERT(datetimeoffset(6), ?)",
        ),
    ),
)
def test_mssql_temporal_renderer_pins_the_catalog_proven_parameter_type(
    literal_type: str,
    literal: str,
    source_type: str,
    target_type: str,
    expected: str,
) -> None:
    scope = parse_portable_relation_scope(
        {
            "kind": "equality",
            "column": "boundary",
            "value": {"type": literal_type, "value": literal},
        }
    )
    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract("boundary", source_type, None, "boundary", target_type, None),
    )
    rendered = render_mssql_portable_scope(scope, binding, quote_identifier=_mssql_quote)
    assert rendered.sql == f"[boundary] = {expected}"
    assert len(rendered.params) == 1
    if literal_type == "timestamptz":
        assert rendered.params == ("2026-08-16T00:00:00Z",)


def test_direct_timestamptz_ast_normalizes_to_utc_before_digest() -> None:
    literal = PortableLiteral("timestamptz", datetime(2026, 8, 16, tzinfo=UTC))
    scope = PortableEqualityScope("occurred_at", literal)
    assert portable_scope_canonical_json(scope).endswith(
        '"value":{"type":"timestamptz","value":"2026-08-16T00:00:00Z"},"version":1}'
    )


def test_direct_timestamptz_ast_never_exposes_timezone_overflow() -> None:
    class OverflowingTimezone(tzinfo):
        def utcoffset(self, value: datetime | None) -> timedelta:
            raise OverflowError("provider overflow must remain internal")

        def dst(self, value: datetime | None) -> timedelta:
            return timedelta(0)

    with pytest.raises(PortableScopeContractError, match="timestamptz_range"):
        PortableLiteral("timestamptz", datetime(2026, 8, 16, tzinfo=OverflowingTimezone()))
