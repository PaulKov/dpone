from __future__ import annotations

from copy import deepcopy

import pytest

from dpone.runtime.state.mssql import MSSQLXMinStateStorage
from dpone.runtime.state.mssql_atomic_catalog import (
    MssqlAtomicStateTables,
    _bindings,
    require_external_atomic_state_catalog,
)
from dpone.runtime.state.mssql_catalog_integrity import (
    MssqlCheckContract,
    MssqlDefaultContract,
    MssqlForeignKeyContract,
    MssqlIndexContract,
    MssqlTableIntegrityContract,
    require_table_catalog_integrity,
)
from dpone.runtime.state.mssql_contract import (
    MssqlColumnShape,
    MssqlExternalTableContract,
    exact_external_table_contract,
    require_external_table_shape,
)
from tests.mssql_catalog_exactness_test_support import (
    EXACT_COLUMN_SAFETY_DRIFTS,
    ExactCatalogExecutor,
    ExactShapeCatalogExecutor,
    add_unexpected_catalog_object,
    catalog_rows,
    expected_exactness_error,
)

_TABLES = MssqlAtomicStateTables(
    state="dpone_source_state",
    receipt="dpone_commit_receipt",
    repair_authority="dpone_repair_authority",
    repair_consumption="dpone_repair_authority_consumption",
    run="dpone_run_state",
    audit="dpone_load_audit",
)
_TABLE_NAMES = tuple(binding.name for binding in _bindings("dbo", _TABLES))
_SHAPE_CASES = tuple(
    (binding.name, shape.name) for binding in _bindings("dbo", _TABLES) for shape in binding.shape.shapes
)

_CONTRACT = MssqlTableIntegrityContract(
    indexes=(
        MssqlIndexContract(kind="primary_key", columns=("id",), clustered=True),
        MssqlIndexContract(
            kind="unique_constraint",
            columns=("state_key", "load_id"),
            clustered=False,
        ),
    ),
    foreign_keys=(
        MssqlForeignKeyContract(
            columns=("state_key",),
            referenced_schema="dbo",
            referenced_table="dpone_source_state",
            referenced_columns=("state_key",),
        ),
    ),
    checks=(
        MssqlCheckContract(
            label="delete_ratio",
            expression="[observed_delete_ratio] BETWEEN 0.0 AND 1.0",
        ),
    ),
    defaults=(MssqlDefaultContract(column="created_at_utc", expression="SYSUTCDATETIME()"),),
    exact_indexes=True,
    exact_foreign_keys=True,
    exact_checks=True,
    exact_defaults=True,
)


def test_exact_identity_factory_cannot_broaden_additive_contracts() -> None:
    additive = MssqlExternalTableContract(
        columns=frozenset({"id"}),
        unique_indexes=(("id",),),
        shapes=(MssqlColumnShape("id", "bigint", 8, 19, 0, False),),
    )
    assert additive.shapes[0].identity is None

    assert additive.exact_columns is False


def test_exact_identity_factory_requires_full_shape_coverage() -> None:

    with pytest.raises(ValueError, match="shape_coverage:missing=payload"):
        exact_external_table_contract(
            columns=frozenset({"id", "payload"}),
            unique_indexes=(("id",),),
            shapes=(MssqlColumnShape("id", "bigint", 8, 19, 0, False),),
            identity_columns=frozenset({"id"}),
        )


def test_exact_identity_factory_rejects_conflicting_declarations() -> None:
    with pytest.raises(ValueError, match="identity_conflict:id"):
        exact_external_table_contract(
            columns=frozenset({"id"}),
            unique_indexes=(("id",),),
            shapes=(MssqlColumnShape("id", "bigint", 8, 19, 0, False, identity=True),),
        )


def _valid_rows() -> dict[str, list[dict[str, object]]]:
    return {
        "indexes": [
            {
                "index_id": 1,
                "index_name": "renamed_pk",
                "is_unique": True,
                "is_primary_key": True,
                "is_unique_constraint": False,
                "is_disabled": False,
                "is_hypothetical": False,
                "ignore_dup_key": False,
                "has_filter": False,
                "filter_definition": None,
                "type_desc": "CLUSTERED",
                "column_name": "id",
                "key_ordinal": 1,
                "is_included_column": False,
                "is_descending_key": False,
            },
            {
                "index_id": 2,
                "index_name": "renamed_uq",
                "is_unique": True,
                "is_primary_key": False,
                "is_unique_constraint": True,
                "is_disabled": False,
                "is_hypothetical": False,
                "ignore_dup_key": False,
                "has_filter": False,
                "filter_definition": None,
                "type_desc": "NONCLUSTERED",
                "column_name": "state_key",
                "key_ordinal": 1,
                "is_included_column": False,
                "is_descending_key": False,
            },
            {
                "index_id": 2,
                "index_name": "renamed_uq",
                "is_unique": True,
                "is_primary_key": False,
                "is_unique_constraint": True,
                "is_disabled": False,
                "is_hypothetical": False,
                "ignore_dup_key": False,
                "has_filter": False,
                "filter_definition": None,
                "type_desc": "NONCLUSTERED",
                "column_name": "load_id",
                "key_ordinal": 2,
                "is_included_column": False,
                "is_descending_key": False,
            },
        ],
        "foreign_keys": [
            {
                "constraint_object_id": 21,
                "constraint_name": "renamed_fk",
                "is_disabled": False,
                "is_not_trusted": False,
                "is_not_for_replication": False,
                "delete_referential_action_desc": "NO_ACTION",
                "update_referential_action_desc": "NO_ACTION",
                "referenced_schema": "dbo",
                "referenced_table": "dpone_source_state",
                "parent_column": "state_key",
                "referenced_column": "state_key",
                "constraint_column_id": 1,
            },
        ],
        "checks": [
            {
                "constraint_name": "renamed_check",
                "definition": "(((1.0) >= [observed_delete_ratio]) AND ((0.0) <= [observed_delete_ratio]))",
                "is_disabled": False,
                "is_not_trusted": False,
                "is_not_for_replication": False,
            },
        ],
        "defaults": [
            {
                "column_name": "created_at_utc",
                "definition": "((sysutcdatetime()))",
            },
        ],
        "partitions": [
            {"index_id": 1, "partition_number": 1, "data_compression_desc": "NONE"},
            {"index_id": 2, "partition_number": 1, "data_compression_desc": "NONE"},
        ],
    }


class _CatalogExecutor:
    def __init__(self, rows: dict[str, list[dict[str, object]]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def get_records(self, query, params=None, as_dict=False):
        del as_dict
        rendered = str(query)
        values = tuple(params or ())
        self.calls.append((rendered, values))
        if "sys.partitions" in rendered:
            return deepcopy(self.rows["partitions"])
        if "sys.foreign_keys" in rendered:
            return deepcopy(self.rows["foreign_keys"])
        if "sys.check_constraints" in rendered:
            return deepcopy(self.rows["checks"])
        if "sys.default_constraints" in rendered:
            return deepcopy(self.rows["defaults"])
        if "sys.indexes" in rendered:
            return deepcopy(self.rows["indexes"])
        raise AssertionError(f"unexpected catalog query: {rendered}")

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"


def _require(rows: dict[str, list[dict[str, object]]]) -> None:
    require_table_catalog_integrity(
        _CatalogExecutor(rows),
        database="Example_System",
        schema="dbo",
        table="dpone_repair_authority_consumption",
        contract=_CONTRACT,
    )


def test_catalog_integrity_accepts_renamed_constraints_and_sql_server_expression_normalization() -> None:
    _require(_valid_rows())


@pytest.mark.parametrize(
    ("drift", "error_code"),
    [
        ("missing_index", "index_missing"),
        ("wrong_index_order", "index_missing"),
        ("case_variant_index_column", "index_missing"),
        ("wrong_index_kind", "index_missing"),
        ("wrong_index_storage", "index_missing"),
        ("non_unique_index", "index_missing"),
        ("ignore_duplicate_keys", "index_missing"),
        ("included_column", "index_missing"),
        ("filtered_key", "index_missing"),
        ("disabled_index", "index_missing"),
        ("hypothetical_index", "index_missing"),
        ("descending_key", "index_missing"),
        ("missing_fk", "foreign_key_missing"),
        ("wrong_fk_target", "foreign_key_missing"),
        ("wrong_fk_column", "foreign_key_missing"),
        ("cascading_fk", "foreign_key_missing"),
        ("disabled_fk", "foreign_key_untrusted_or_disabled"),
        ("untrusted_fk", "foreign_key_untrusted_or_disabled"),
        ("replication_bypass_fk", "foreign_key_not_for_replication"),
        ("missing_check", "check_missing"),
        ("wrong_check", "check_missing"),
        ("disabled_check", "check_untrusted_or_disabled"),
        ("untrusted_check", "check_untrusted_or_disabled"),
        ("replication_bypass_check", "check_not_for_replication"),
        ("missing_default", "default_missing"),
        ("wrong_default", "default_missing"),
        ("extra_index", "index_unexpected"),
        ("extra_fk", "foreign_key_unexpected"),
        ("extra_check", "check_unexpected"),
        ("extra_default", "default_unexpected"),
        ("page_compression", "storage_contract"),
        ("second_partition", "storage_contract"),
    ],
)
def test_catalog_integrity_fails_closed_for_safety_critical_drift(drift: str, error_code: str) -> None:
    rows = _valid_rows()
    if drift == "missing_index":
        rows["indexes"] = rows["indexes"][:1]
    elif drift == "wrong_index_order":
        rows["indexes"][1]["column_name"], rows["indexes"][2]["column_name"] = (
            rows["indexes"][2]["column_name"],
            rows["indexes"][1]["column_name"],
        )
    elif drift == "case_variant_index_column":
        rows["indexes"][1]["column_name"] = "State_Key"
    elif drift == "wrong_index_kind":
        rows["indexes"][1]["is_unique_constraint"] = False
    elif drift == "wrong_index_storage":
        rows["indexes"][1]["type_desc"] = "CLUSTERED"
    elif drift == "non_unique_index":
        rows["indexes"][1]["is_unique"] = False
    elif drift == "ignore_duplicate_keys":
        rows["indexes"][1]["ignore_dup_key"] = True
    elif drift == "included_column":
        rows["indexes"][1]["is_included_column"] = True
    elif drift == "filtered_key":
        rows["indexes"][1].update({"has_filter": True, "filter_definition": "[state_key] IS NOT NULL"})
    elif drift == "disabled_index":
        rows["indexes"][1]["is_disabled"] = True
    elif drift == "hypothetical_index":
        rows["indexes"][1]["is_hypothetical"] = True
    elif drift == "descending_key":
        rows["indexes"][1]["is_descending_key"] = True
    elif drift == "missing_fk":
        rows["foreign_keys"] = []
    elif drift == "wrong_fk_target":
        rows["foreign_keys"][0]["referenced_table"] = "wrong_state"
    elif drift == "wrong_fk_column":
        rows["foreign_keys"][0]["referenced_column"] = "wrong_key"
    elif drift == "cascading_fk":
        rows["foreign_keys"][0]["delete_referential_action_desc"] = "CASCADE"
    elif drift == "disabled_fk":
        rows["foreign_keys"][0]["is_disabled"] = True
    elif drift == "untrusted_fk":
        rows["foreign_keys"][0]["is_not_trusted"] = True
    elif drift == "replication_bypass_fk":
        rows["foreign_keys"][0]["is_not_for_replication"] = True
    elif drift == "missing_check":
        rows["checks"] = []
    elif drift == "wrong_check":
        rows["checks"][0]["definition"] = "[observed_delete_ratio] BETWEEN 0.0 AND 0.9"
    elif drift == "disabled_check":
        rows["checks"][0]["is_disabled"] = True
    elif drift == "untrusted_check":
        rows["checks"][0]["is_not_trusted"] = True
    elif drift == "replication_bypass_check":
        rows["checks"][0]["is_not_for_replication"] = True
    elif drift == "missing_default":
        rows["defaults"] = []
    elif drift == "wrong_default":
        rows["defaults"][0]["definition"] = "GETUTCDATE()"
    elif drift == "extra_index":
        extra = deepcopy(rows["indexes"][0])
        extra.update(
            {
                "index_id": 3,
                "index_name": "unexpected_index",
                "is_primary_key": False,
                "is_unique": False,
                "column_name": "state_key",
            }
        )
        rows["indexes"].append(extra)
        rows["partitions"].append({"index_id": 3, "partition_number": 1, "data_compression_desc": "NONE"})
    elif drift == "extra_fk":
        extra = deepcopy(rows["foreign_keys"][0])
        extra.update({"constraint_object_id": 22, "constraint_name": "unexpected_fk"})
        rows["foreign_keys"].append(extra)
    elif drift == "extra_check":
        rows["checks"].append(
            {
                "constraint_name": "unexpected_check",
                "definition": "[id] >= 0",
                "is_disabled": False,
                "is_not_trusted": False,
                "is_not_for_replication": False,
            }
        )
    elif drift == "extra_default":
        rows["defaults"].append(
            {
                "column_name": "id",
                "definition": "0",
            }
        )
    elif drift == "page_compression":
        rows["partitions"][0]["data_compression_desc"] = "PAGE"
    elif drift == "second_partition":
        rows["partitions"].append({"index_id": 1, "partition_number": 2, "data_compression_desc": "NONE"})
    else:  # pragma: no cover - exhaustive test fixture guard
        raise AssertionError(drift)

    with pytest.raises(RuntimeError, match=error_code):
        _require(rows)


def test_catalog_predicate_parser_rejects_comments_instead_of_token_matching() -> None:
    rows = _valid_rows()
    rows["checks"][0]["definition"] = "[observed_delete_ratio] <= 0.9 /* observed_delete_ratio BETWEEN 0 AND 1 */"

    with pytest.raises(RuntimeError, match="check_missing"):
        _require(rows)


def test_target_atomic_storage_passes_all_six_exact_locations_to_catalog_preflight(monkeypatch) -> None:
    observed: list[tuple[object, object, object, object]] = []

    def capture(connector, *, database, schema, tables) -> None:
        observed.append((connector, database, schema, tables))

    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_external_atomic_state_catalog",
        capture,
    )
    connector = object()
    storage = MSSQLXMinStateStorage(
        connector,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        repair_authority_table="dpone_repair_authority",
        repair_consumption_table="dpone_repair_authority_consumption",
        run_table="dpone_run_state",
        audit_table="dpone_load_audit",
        atomicity="target_atomic",
        provisioning="external",
    )

    storage.preflight_atomic_catalog()

    assert observed == [
        (
            connector,
            "Example_System",
            "dbo",
            MssqlAtomicStateTables(
                state="dpone_source_state",
                receipt="dpone_commit_receipt",
                repair_authority="dpone_repair_authority",
                repair_consumption="dpone_repair_authority_consumption",
                run="dpone_run_state",
                audit="dpone_load_audit",
            ),
        )
    ]


def test_atomic_catalog_declares_integrity_for_all_six_reference_tables(monkeypatch) -> None:
    shapes: list[str] = []
    integrity: dict[str, MssqlTableIntegrityContract] = {}
    triggers: list[str] = []
    trigger_sets: dict[str, frozenset[str]] = {}

    def capture_shape(_connector, *, database, schema, table, contract) -> None:
        del database, schema, contract
        shapes.append(table)

    def capture_integrity(_connector, *, database, schema, table, contract) -> None:
        del database, schema
        integrity[table] = contract

    def capture_trigger(_connector, *, database, schema, table) -> None:
        del database, schema
        triggers.append(table)

    def capture_trigger_set(_connector, *, database, schema, table, triggers, error_code) -> None:
        del database, schema, error_code
        trigger_sets[table] = triggers

    monkeypatch.setattr("dpone.runtime.state.mssql_atomic_catalog.require_external_table_shape", capture_shape)
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_table_catalog_integrity",
        capture_integrity,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_immutable_authority_trigger",
        capture_trigger,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_exact_table_trigger_set",
        capture_trigger_set,
    )

    require_external_atomic_state_catalog(
        object(),
        database="Example_System",
        schema="dbo",
        tables=_TABLES,
    )

    assert (
        shapes
        == list(integrity)
        == [
            "dpone_source_state",
            "dpone_commit_receipt",
            "dpone_repair_authority",
            "dpone_repair_authority_consumption",
            "dpone_run_state",
            "dpone_load_audit",
        ]
    )
    assert triggers == ["dpone_repair_authority"]
    assert trigger_sets == {
        "dpone_source_state": frozenset(),
        "dpone_commit_receipt": frozenset(),
        "dpone_repair_authority": frozenset({"trg_dpone_repair_authority_immutable"}),
        "dpone_repair_authority_consumption": frozenset(),
        "dpone_run_state": frozenset(),
        "dpone_load_audit": frozenset(),
    }
    assert [index.kind for index in integrity[_TABLES.state].indexes] == ["primary_key", "unique_index"]
    assert len(integrity[_TABLES.receipt].foreign_keys) == 1
    assert {check.label for check in integrity[_TABLES.repair_authority].checks} == {
        "checkpoint",
        "transfer",
        "delete_rows",
        "delete_ratio",
    }
    assert len(integrity[_TABLES.repair_consumption].foreign_keys) == 3
    assert {default.column for default in integrity[_TABLES.run].defaults} == {
        "__dpone__loaded_at",
        "__dpone__updated_at",
    }
    assert {default.column for default in integrity[_TABLES.audit].defaults} == {"__dpone__loaded_at"}
    assert all(
        contract.exact_indexes and contract.exact_foreign_keys and contract.exact_checks and contract.exact_defaults
        for contract in integrity.values()
    )
    assert all(contract.require_none_compression for contract in integrity.values())
    assert all(contract.require_single_partition for contract in integrity.values())


@pytest.mark.parametrize("table_name", _TABLE_NAMES)
@pytest.mark.parametrize("family", ("index", "foreign_key", "check", "default"))
def test_each_atomic_table_rejects_unexpected_catalog_objects(table_name: str, family: str) -> None:
    contract = next(binding.integrity for binding in _bindings("dbo", _TABLES) if binding.name == table_name)
    rows = catalog_rows(contract)
    add_unexpected_catalog_object(rows, family)

    with pytest.raises(RuntimeError, match=expected_exactness_error(family)):
        require_table_catalog_integrity(
            ExactCatalogExecutor(rows),
            database="Example_System",
            schema="dbo",
            table=table_name,
            contract=contract,
        )


@pytest.mark.parametrize("table_name", _TABLE_NAMES)
def test_each_atomic_table_declares_identity_for_every_column(table_name: str) -> None:
    contract = next(binding.shape for binding in _bindings("dbo", _TABLES) if binding.name == table_name)
    expected_identity_columns = {"id"} if table_name == _TABLES.run else set()

    assert all(shape.identity is not None for shape in contract.shapes)
    assert all(
        getattr(shape, catalog_field) is not None
        for shape in contract.shapes
        for catalog_field, _drift in EXACT_COLUMN_SAFETY_DRIFTS
    )
    assert all(shape.is_ansi_padded is not None for shape in contract.shapes)
    assert {shape.name for shape in contract.shapes if shape.identity} == expected_identity_columns


def test_exact_shape_query_reads_all_authority_altering_column_metadata() -> None:
    contract = next(binding.shape for binding in _bindings("dbo", _TABLES) if binding.name == _TABLES.state)
    executor = ExactShapeCatalogExecutor(contract)

    require_external_table_shape(
        executor,
        database="Example_System",
        schema="dbo",
        table=_TABLES.state,
        contract=contract,
    )

    column_query = next(query for query in executor.queries if "sys.columns AS c" in query)
    assert all(
        token in column_query
        for token in (
            "c.is_computed",
            "c.is_sparse",
            "c.is_rowguidcol",
            "c.generated_always_type",
            "c.is_hidden",
            "c.is_masked",
            "c.encryption_type",
            "c.is_ansi_padded",
            "c.is_filestream",
            "c.is_column_set",
            "c.collation_name",
            "DATABASEPROPERTYEX",
            "ty.is_user_defined",
            "ty.is_assembly_type",
            "c.rule_object_id",
            "c.default_object_id",
            "sys.default_constraints",
        )
    )


@pytest.mark.parametrize(("table_name", "column_name"), _SHAPE_CASES)
def test_each_atomic_column_rejects_identity_drift(table_name: str, column_name: str) -> None:
    contract = next(binding.shape for binding in _bindings("dbo", _TABLES) if binding.name == table_name)
    expected_identity = next(shape.identity for shape in contract.shapes if shape.name == column_name)
    assert expected_identity is not None

    with pytest.raises(
        RuntimeError,
        match=rf"mssql_external_state_contract_shape:.*{table_name}:{column_name}",
    ):
        require_external_table_shape(
            ExactShapeCatalogExecutor(
                contract,
                column_overrides={column_name: {"is_identity": not expected_identity}},
            ),
            database="Example_System",
            schema="dbo",
            table=table_name,
            contract=contract,
        )


@pytest.mark.parametrize("table_name", _TABLE_NAMES)
@pytest.mark.parametrize(("catalog_field", "drift"), EXACT_COLUMN_SAFETY_DRIFTS)
def test_each_atomic_table_rejects_unsafe_column_catalog_features(
    table_name: str,
    catalog_field: str,
    drift: object,
) -> None:
    contract = next(binding.shape for binding in _bindings("dbo", _TABLES) if binding.name == table_name)
    column_name = contract.shapes[0].name

    with pytest.raises(
        RuntimeError,
        match=rf"mssql_external_state_contract_shape:.*{table_name}:{column_name}",
    ):
        require_external_table_shape(
            ExactShapeCatalogExecutor(
                contract,
                column_overrides={column_name: {catalog_field: drift}},
            ),
            database="Example_System",
            schema="dbo",
            table=table_name,
            contract=contract,
        )


@pytest.mark.parametrize("table_name", _TABLE_NAMES)
def test_each_atomic_table_rejects_ansi_padding_drift(table_name: str) -> None:
    contract = next(binding.shape for binding in _bindings("dbo", _TABLES) if binding.name == table_name)
    shape = contract.shapes[0]
    assert shape.is_ansi_padded is not None

    with pytest.raises(
        RuntimeError,
        match=rf"mssql_external_state_contract_shape:.*{table_name}:{shape.name}",
    ):
        require_external_table_shape(
            ExactShapeCatalogExecutor(
                contract,
                column_overrides={shape.name: {"is_ansi_padded": not shape.is_ansi_padded}},
            ),
            database="Example_System",
            schema="dbo",
            table=table_name,
            contract=contract,
        )


@pytest.mark.parametrize("table_name", _TABLE_NAMES)
def test_each_atomic_table_rejects_an_unexpected_trigger(
    monkeypatch: pytest.MonkeyPatch,
    table_name: str,
) -> None:
    class _TriggerCatalog:
        @staticmethod
        def quote_identifier(value: str) -> str:
            return f"[{value}]"

        @staticmethod
        def get_records(query, params=None, as_dict=False):
            del as_dict
            if "sys.triggers" not in str(query):
                raise AssertionError(f"unexpected catalog query: {query}")
            queried_table = tuple(params or ())[1]
            if queried_table != table_name:
                return (
                    [{"trigger_name": "trg_dpone_repair_authority_immutable"}]
                    if queried_table == _TABLES.repair_authority
                    else []
                )
            rows = [{"trigger_name": "unexpected_trigger", "is_disabled": True}]
            if queried_table == _TABLES.repair_authority:
                rows.append({"trigger_name": "trg_dpone_repair_authority_immutable"})
            return rows

    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_external_table_shape",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_table_catalog_integrity",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_immutable_authority_trigger",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match=f"mssql_external_state_trigger_set:.*{table_name}"):
        require_external_atomic_state_catalog(
            _TriggerCatalog(),
            database="Example_System",
            schema="dbo",
            tables=_TABLES,
        )


def test_repair_authority_rejects_case_variant_and_disabled_sole_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _TriggerCatalog:
        def __init__(self, *, case_variant: bool) -> None:
            self.case_variant = case_variant

        @staticmethod
        def quote_identifier(value: str) -> str:
            return f"[{value}]"

        def get_records(self, query, params=None, as_dict=False):
            del as_dict
            rendered = str(query)
            queried_table = tuple(params or ())[1]
            if "sys.sql_modules" not in rendered:
                if queried_table != _TABLES.repair_authority:
                    return []
                name = (
                    "Trg_Dpone_Repair_Authority_Immutable"
                    if self.case_variant
                    else "trg_dpone_repair_authority_immutable"
                )
                return [{"trigger_name": name}]
            return [
                {
                    "trigger_name": "trg_dpone_repair_authority_immutable",
                    "is_disabled": True,
                    "is_instead_of_trigger": True,
                    "trigger_definition": (
                        "CREATE TRIGGER t ON x INSTEAD OF UPDATE, DELETE AS "
                        "THROW 51000, 'DPONE_REPAIR_AUTHORITY_IMMUTABLE', 1;"
                    ),
                    "event_type": event,
                }
                for event in ("UPDATE", "DELETE")
            ]

    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_external_table_shape",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_atomic_catalog.require_table_catalog_integrity",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="mssql_external_state_trigger_set"):
        require_external_atomic_state_catalog(
            _TriggerCatalog(case_variant=True),
            database="Example_System",
            schema="dbo",
            tables=_TABLES,
        )
    with pytest.raises(RuntimeError, match="immutable_trigger"):
        require_external_atomic_state_catalog(
            _TriggerCatalog(case_variant=False),
            database="Example_System",
            schema="dbo",
            tables=_TABLES,
        )


@pytest.mark.parametrize(
    ("atomicity", "provisioning"),
    [("after_target", "external"), ("target_atomic", "runtime")],
)
def test_complete_catalog_gate_is_not_reused_outside_external_target_atomic_routes(
    atomicity: str,
    provisioning: str,
) -> None:
    storage = MSSQLXMinStateStorage(
        object(),
        database="DWH_Dev",
        schema="system",
        audit_table="dpone_load_audit",
        atomicity=atomicity,
        provisioning=provisioning,
    )

    with pytest.raises(RuntimeError, match="external_provisioning_required"):
        storage.preflight_atomic_catalog()
