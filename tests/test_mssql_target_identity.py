from __future__ import annotations

from uuid import UUID

import pytest

from dpone.runtime.state import mssql_target_identity as subject
from dpone.runtime.state import mssql_target_identity_contract as contract_subject
from dpone.runtime.state.mssql_catalog_integrity import require_table_catalog_integrity
from dpone.runtime.state.mssql_contract import require_external_table_shape
from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity
from dpone.runtime.state.mssql_target_identity import (
    TARGET_IDENTITY_REGISTRY_CONTRACT,
    TARGET_IDENTITY_REGISTRY_INTEGRITY,
    MssqlPhysicalTargetIdentityError,
    resolve_mssql_physical_target_identity,
)
from dpone.runtime.state.mssql_target_identity_contract import require_target_identity_registry_contract
from tests.mssql_catalog_exactness_test_support import (
    EXACT_COLUMN_SAFETY_DRIFTS,
    ExactCatalogExecutor,
    ExactShapeCatalogExecutor,
    add_unexpected_catalog_object,
    catalog_rows,
    expected_exactness_error,
)

_BINDING_A = UUID("11111111-1111-1111-1111-111111111111")
_BINDING_B = UUID("22222222-2222-2222-2222-222222222222")
_SESSION = MssqlSessionIdentity(
    server_name="DWH-LISTENER",
    machine_name="SQLNODE01",
    instance_name="MSSQLSERVER",
    replica_name="SQLNODE01",
    effective_principal="dpone_runtime",
    original_login="dpone_runtime",
)


class _RegistryConnector:
    def __init__(self, *, case_sensitive: bool, object_type: str | None = "U") -> None:
        self.case_sensitive = case_sensitive
        self.object_type = object_type
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def get_records(self, query, params=None, as_dict=False):
        del as_dict
        rendered = str(query)
        values = tuple(params or ())
        self.calls.append((rendered, values))
        if "FROM sys.databases" in rendered:
            return [
                {
                    "database_id": 7,
                    "database_name": "dwh_example",
                    "collation_name": "Latin1_General_100_CS_AS" if self.case_sensitive else "Latin1_General_100_CI_AS",
                    "database_create_token": "2026-01-01T00:00:00",
                }
            ]
        if "FROM [dwh_example].[dbo].[dpone_target_identity]" in rendered:
            requested_schema, requested_table = (str(value) for value in values)
            if requested_schema.casefold() != "sample_metrics":
                return []
            if self.case_sensitive:
                bindings = {
                    "Metrics_Value": (_BINDING_A, "Metrics_Value"),
                    "metrics_value": (_BINDING_B, "metrics_value"),
                }
                binding = bindings.get(requested_table)
            else:
                binding = (_BINDING_A, "Metrics_Value") if requested_table.casefold() == "metrics_value" else None
            if binding is None:
                return []
            binding_id, canonical_name = binding
            return [
                {
                    "binding_id": str(binding_id),
                    "schema_name": "sample_metrics",
                    "table_name": canonical_name,
                    "object_id": 42 if self.object_type is not None else None,
                    "object_type": self.object_type,
                }
            ]
        return []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        return ".".join(f"[{part}]" for part in (database, schema, table) if part)


@pytest.fixture(autouse=True)
def _isolate_registry_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "require_target_identity_registry_contract", lambda *_args, **_kwargs: None)


def _resolve(connector: _RegistryConnector, table: str):
    return resolve_mssql_physical_target_identity(
        connector,
        session=_SESSION,
        database="dwh_example",
        schema="SAMPLE_METRICS",
        table=table,
    )


def test_ci_registry_aliases_converge_on_one_binary_identity() -> None:
    connector = _RegistryConnector(case_sensitive=False)

    lower = _resolve(connector, "metrics_value")
    upper = _resolve(connector, "METRICS_VALUE")

    assert lower.digest == upper.digest
    assert lower.table_name == upper.table_name == "Metrics_Value"
    assert len(lower.digest) == 32


def test_cs_registry_preserves_distinct_case_sensitive_objects() -> None:
    connector = _RegistryConnector(case_sensitive=True)

    mixed = _resolve(connector, "Metrics_Value")
    lower = _resolve(connector, "metrics_value")

    assert mixed.binding_id != lower.binding_id
    assert mixed.digest != lower.digest


def test_registry_missing_binding_and_non_table_fail_closed() -> None:
    connector = _RegistryConnector(case_sensitive=True)
    with pytest.raises(MssqlPhysicalTargetIdentityError, match="binding_missing_or_ambiguous"):
        _resolve(connector, "METRICS_VALUE")

    connector = _RegistryConnector(case_sensitive=False, object_type="V")
    with pytest.raises(MssqlPhysicalTargetIdentityError, match="is_not_table"):
        _resolve(connector, "metrics_value")


def test_registry_contract_is_exact_collation_backed_rowstore() -> None:
    assert TARGET_IDENTITY_REGISTRY_CONTRACT.exact_columns is True
    assert {shape.name for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes} == {
        "binding_id",
        "schema_name",
        "table_name",
        "created_at_utc",
    }
    assert tuple(index.columns for index in TARGET_IDENTITY_REGISTRY_INTEGRITY.indexes) == (
        ("binding_id",),
        ("schema_name", "table_name"),
    )
    assert TARGET_IDENTITY_REGISTRY_INTEGRITY.require_none_compression is True
    assert TARGET_IDENTITY_REGISTRY_INTEGRITY.require_single_partition is True
    assert TARGET_IDENTITY_REGISTRY_INTEGRITY.exact_indexes is True
    assert TARGET_IDENTITY_REGISTRY_INTEGRITY.exact_foreign_keys is True
    assert TARGET_IDENTITY_REGISTRY_INTEGRITY.exact_checks is True
    assert TARGET_IDENTITY_REGISTRY_INTEGRITY.exact_defaults is True
    assert all(shape.identity is False for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes)
    assert all(
        getattr(shape, catalog_field) is not None
        for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes
        for catalog_field, _drift in EXACT_COLUMN_SAFETY_DRIFTS
    )
    assert all(shape.is_ansi_padded is not None for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes)


@pytest.mark.parametrize(
    "column_name",
    tuple(shape.name for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes),
)
def test_registry_contract_rejects_identity_drift(column_name: str) -> None:
    with pytest.raises(
        RuntimeError,
        match=rf"mssql_external_state_contract_shape:.*dpone_target_identity:{column_name}",
    ):
        require_external_table_shape(
            ExactShapeCatalogExecutor(
                TARGET_IDENTITY_REGISTRY_CONTRACT,
                column_overrides={column_name: {"is_identity": True}},
            ),
            database="dwh_example",
            schema="dbo",
            table="dpone_target_identity",
            contract=TARGET_IDENTITY_REGISTRY_CONTRACT,
        )


@pytest.mark.parametrize(("catalog_field", "drift"), EXACT_COLUMN_SAFETY_DRIFTS)
def test_registry_contract_rejects_unsafe_column_catalog_features(
    catalog_field: str,
    drift: object,
) -> None:
    column_name = "binding_id"

    with pytest.raises(
        RuntimeError,
        match=rf"mssql_external_state_contract_shape:.*dpone_target_identity:{column_name}",
    ):
        require_external_table_shape(
            ExactShapeCatalogExecutor(
                TARGET_IDENTITY_REGISTRY_CONTRACT,
                column_overrides={column_name: {catalog_field: drift}},
            ),
            database="dwh_example",
            schema="dbo",
            table="dpone_target_identity",
            contract=TARGET_IDENTITY_REGISTRY_CONTRACT,
        )


def test_registry_contract_rejects_ansi_padding_drift() -> None:
    shape = next(shape for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes if shape.name == "schema_name")
    assert shape.is_ansi_padded is True

    with pytest.raises(
        RuntimeError,
        match=r"mssql_external_state_contract_shape:.*dpone_target_identity:schema_name",
    ):
        require_external_table_shape(
            ExactShapeCatalogExecutor(
                TARGET_IDENTITY_REGISTRY_CONTRACT,
                column_overrides={"schema_name": {"is_ansi_padded": False}},
            ),
            database="dwh_example",
            schema="dbo",
            table="dpone_target_identity",
            contract=TARGET_IDENTITY_REGISTRY_CONTRACT,
        )


@pytest.mark.parametrize("family", ("index", "foreign_key", "check", "default"))
def test_registry_contract_rejects_unexpected_catalog_objects(family: str) -> None:
    rows = catalog_rows(TARGET_IDENTITY_REGISTRY_INTEGRITY)
    add_unexpected_catalog_object(rows, family)

    with pytest.raises(RuntimeError, match=expected_exactness_error(family)):
        require_table_catalog_integrity(
            ExactCatalogExecutor(rows),
            database="dwh_example",
            schema="dbo",
            table="dpone_target_identity",
            contract=TARGET_IDENTITY_REGISTRY_INTEGRITY,
        )


@pytest.mark.parametrize("drift", ("extra", "case_variant", "disabled", "not_for_replication"))
def test_registry_contract_rejects_non_exact_or_disabled_trigger(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    class _TriggerCatalog:
        @staticmethod
        def quote_identifier(value: str) -> str:
            return f"[{value}]"

        @staticmethod
        def get_records(query, params=None, as_dict=False):
            del params, as_dict
            rendered = str(query)
            if "sys.columns" in rendered:
                return [
                    {"column_name": "schema_name", "collation_name": "Latin1_General_100_CI_AS"},
                    {"column_name": "table_name", "collation_name": "Latin1_General_100_CI_AS"},
                ]
            if "sys.sql_modules" not in rendered:
                name = (
                    "Trg_Dpone_Target_Identity_Immutable"
                    if drift == "case_variant"
                    else "trg_dpone_target_identity_immutable"
                )
                rows = [{"trigger_name": name}]
                if drift == "extra":
                    rows.append({"trigger_name": "unexpected_trigger"})
                return rows
            return [
                {
                    "trigger_name": "trg_dpone_target_identity_immutable",
                    "is_disabled": drift == "disabled",
                    "is_not_for_replication": drift == "not_for_replication",
                    "is_instead_of_trigger": True,
                    "trigger_definition": (
                        "CREATE TRIGGER t ON x INSTEAD OF UPDATE, DELETE AS "
                        "THROW 51000, 'DPONE_TARGET_IDENTITY_IMMUTABLE', 1;"
                    ),
                    "event_type": event,
                }
                for event in ("UPDATE", "DELETE")
            ]

    monkeypatch.setattr(contract_subject, "require_external_table_shape", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(contract_subject, "require_table_catalog_integrity", lambda *_args, **_kwargs: None)

    expected_error = "immutable_trigger" if drift in {"disabled", "not_for_replication"} else "registry_trigger_set"
    with pytest.raises(RuntimeError, match=expected_error):
        require_target_identity_registry_contract(
            _TriggerCatalog(),
            database="dwh_example",
            database_collation="Latin1_General_100_CI_AS",
        )


def test_registry_contract_rejects_case_variant_decoy_columns_before_constraints() -> None:
    class _DecoyCatalog:
        @staticmethod
        def quote_identifier(value: str) -> str:
            return f"[{value}]"

        @staticmethod
        def get_records(query, params=None, as_dict=False):
            del params, as_dict
            if "sys.columns" not in str(query):
                raise AssertionError("index catalog must not be trusted after ambiguous columns")
            shapes = {shape.name: shape for shape in TARGET_IDENTITY_REGISTRY_CONTRACT.shapes}
            rows = [
                {
                    "column_name": name,
                    "type_name": shape.type_name,
                    "max_length": shape.max_length,
                    "precision": shape.precision,
                    "scale": shape.scale,
                    "is_nullable": shape.nullable,
                    "is_identity": False,
                }
                for name, shape in shapes.items()
            ]
            return [*rows, {**rows[0], "column_name": "Binding_ID"}]

    with pytest.raises(RuntimeError, match="identifier_case_ambiguity"):
        require_external_table_shape(
            _DecoyCatalog(),
            database="dwh_example",
            schema="dbo",
            table="dpone_target_identity",
            contract=TARGET_IDENTITY_REGISTRY_CONTRACT,
        )
