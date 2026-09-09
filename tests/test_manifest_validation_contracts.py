from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.contracts.technical_columns import TechnicalColumnsMode
from dpone.manifest.models import LoadedManifest
from dpone.manifest.validation import Severity, ValidationProfile, has_errors, validate_manifest

_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def process_spec(
    *,
    name: str = "load_orders",
    selector: str = "public.orders",
    target_schema: str = "landing__demo__demo_db",
    target_table: str = "public__orders",
    options: dict | None = None,
    reconciliation: bool = False,
    raw_config: dict | None = None,
    load_strategy: str | None = None,
) -> SimpleNamespace:
    load_config_values: dict[str, object] = {
        "target_schema": target_schema,
        "target_table": target_table,
        "options": options or {},
        "reconciliation": reconciliation,
    }
    if load_strategy is not None:
        load_config_values["load_strategy"] = SimpleNamespace(value=load_strategy)
    return SimpleNamespace(
        name=name,
        selector=selector,
        config=SimpleNamespace(load_config=SimpleNamespace(**load_config_values)),
        raw_config=raw_config
        or {
            "source": {
                "table": {
                    "schema": "public",
                    "name": "orders",
                }
            }
        },
    )


def loaded_manifest(*, raw: dict | None = None, processes: tuple[SimpleNamespace, ...]) -> LoadedManifest:
    return LoadedManifest(
        path=Path("/repo/manifests/batch.yaml"),
        kind="dpone.batch.v1",
        raw=raw or {},
        processes=processes,
    )


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


@pytest.mark.parametrize(
    ("source_type", "sink_type"),
    (("postgres", "mssql"), ("PostgreSQL", "odbc")),
)
def test_manifest_validates_explicit_xmin_initial_and_incremental_phases(
    source_type: str,
    sink_type: str,
) -> None:
    initial = process_spec(
        name="orders_initial",
        load_strategy="backfill",
        options={
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
            "backfill": {
                "inner_mode": "incremental_merge",
                "chunk": {"column": "id", "from": 1, "to": 10, "step": 1},
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
            },
            "unique_key": ["id"],
        },
        raw_config={"source": {"type": source_type}, "sink": {"type": sink_type}},
    )
    initial.config.load_config.reconciliation_policy = None
    incremental = process_spec(
        name="orders_incremental",
        load_strategy="incremental_merge",
        options={
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "incremental", "handoff_id": "orders_v1"},
        },
        raw_config={"source": {"type": source_type}, "sink": {"type": sink_type}},
    )
    incremental.config.load_config.reconciliation_policy = {"enabled": True, "mode": "key_snapshot"}

    issues = validate_manifest(loaded_manifest(processes=(initial, incremental)))

    assert not any(issue.code.startswith("POSTGRES_XMIN_") for issue in issues)


def test_manifest_rejects_incremental_phase_without_key_snapshot_contract() -> None:
    spec = process_spec(
        load_strategy="incremental_merge",
        options={
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "incremental", "handoff_id": "orders_v1"},
        },
        raw_config={"source": {"type": "postgres"}, "sink": {"type": "mssql"}},
    )
    spec.config.load_config.reconciliation_policy = None

    issues = validate_manifest(loaded_manifest(processes=(spec,)))

    assert "POSTGRES_XMIN_INCREMENTAL_CONTRACT_INVALID" in issue_codes(issues)


def test_validate_manifest_is_noop_without_profile_or_manifest_validation_block() -> None:
    manifest = loaded_manifest(processes=(process_spec(),))

    assert validate_manifest(manifest) == []


def test_validate_manifest_reports_naming_label_description_and_technical_column_issues() -> None:
    profile = ValidationProfile(
        name="strict",
        dataset_pattern=r"^landing__[a-z]+__[a-z]+$",
        table_pattern=r"^[a-z]+__[a-z]+$",
        required_labels=("layer", "src", "db", "schema", "host", "ingest"),
        require_table_description=True,
        technical_columns=TechnicalColumnsMode.REQUIRED,
        missing_labels_severity=Severity.ERROR,
        missing_description_severity=Severity.ERROR,
        label_value_patterns={"layer": r"^landing$"},
        forbidden_label_values={"host": ("unknown",)},
        description_required_regex=(r"(?im)^Источник:\s*.+$",),
        description_forbidden_regex=(r"(?im)^SLA:\s*tbd$",),
        require_source_path_in_description=True,
    )
    manifest = loaded_manifest(
        processes=(
            process_spec(
                target_schema="badDataset",
                target_table="BadTable",
                reconciliation=True,
                options={
                    "technical_columns": "forbidden",
                    "include_technical_columns": "true",
                    "table_labels": {
                        "layer": "raw",
                        "src": "",
                        "db": "demo_db",
                        "schema": "public",
                        "host": "unknown",
                    },
                    "table_description": "SLA: tbd",
                },
            ),
        )
    )

    issues = validate_manifest(manifest, profile=profile)

    assert issue_codes(issues) == [
        "NAMING_DATASET_PATTERN",
        "NAMING_TABLE_PATTERN",
        "TECHNICAL_COLUMNS_CONFLICT",
        "RECONCILIATION_REQUIRES_TECH_COLUMNS",
        "TECHNICAL_COLUMNS_DISABLED",
        "META_LABELS_MISSING",
        "META_LABEL_VALUE_EMPTY",
        "META_LABEL_VALUE_FORBIDDEN",
        "META_LABEL_VALUE_PATTERN",
        "META_DESCRIPTION_REQUIRED_REGEX",
        "META_DESCRIPTION_FORBIDDEN_REGEX",
        "META_DESCRIPTION_SOURCE_PATH",
    ]
    assert has_errors(issues) is True
    assert all(issue.selector == "public.orders" for issue in issues)


def test_validate_manifest_reports_bad_regexes_and_bad_technical_column_mode_from_manifest_profile() -> None:
    manifest = loaded_manifest(
        raw={
            "validation": {
                "dataset_pattern": r"^landing__",
                "required_labels": ["src"],
                "technical_columns": "required",
                "label_value_patterns": {"src": "["},
                "description_required_regex": "[",
                "description_forbidden_regex": "[",
            }
        },
        processes=(
            process_spec(
                options={
                    "technical_columns": "surprising",
                    "table_labels": {"src": "demo"},
                }
            ),
        ),
    )

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == [
        "VALIDATION_BAD_REGEX",
        "VALIDATION_BAD_REGEX",
        "VALIDATION_BAD_REGEX",
        "TECHNICAL_COLUMNS_BAD_MODE",
    ]


def test_validate_manifest_rejects_invalid_manifest_technical_columns_policy() -> None:
    manifest = loaded_manifest(
        raw={
            "validation": {
                "technical_columns": "surprising",
            }
        },
        processes=(process_spec(),),
    )

    with pytest.raises(ValueError, match="validation.technical_columns must be one of"):
        validate_manifest(manifest)


def test_validate_manifest_accepts_valid_landing_description_source_path() -> None:
    profile = ValidationProfile(
        name="landing",
        dataset_pattern=r"^landing__[a-z]+__[a-z_]+$",
        table_pattern=r"^[a-z]+__[a-z]+$",
        required_labels=("db", "schema", "host"),
        require_table_description=True,
        technical_columns=TechnicalColumnsMode.REQUIRED,
        description_required_regex=(
            r"(?im)^Источник:\s*.+$",
            r"(?im)^Владелец:\s*.+$",
            r"(?im)^Контакт:\s*.+$",
            r"(?im)^SLA:\s*.+$",
        ),
        require_source_path_in_description=True,
    )
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={
                    "technical_columns": "required",
                    "table_labels": {
                        "db": "demo_db",
                        "schema": "public",
                        "host": "demo_host",
                    },
                    "table_description": "\n".join(
                        [
                            "Источник: db.example.com/demo_db/public/orders",
                            "Владелец: data-platform",
                            "Контакт: data@example.com",
                            "SLA: daily",
                        ]
                    ),
                }
            ),
        )
    )

    assert validate_manifest(manifest, profile=profile) == []


def test_validate_manifest_reports_label_type_and_source_path_undetermined() -> None:
    profile = ValidationProfile(
        name="labels",
        required_labels=("src",),
        require_table_description=False,
        require_source_path_in_description=True,
        description_required_regex=(r"(?im)^Источник:\s*.+$",),
    )
    manifest = loaded_manifest(
        processes=(
            process_spec(
                target_schema="landing",
                target_table="orders",
                options={
                    "table_labels": ["not", "a", "mapping"],
                    "table_description": "Источник: db.example.com/demo/public/orders",
                },
                raw_config={"source": {"table": {}}},
            ),
        )
    )

    issues = validate_manifest(manifest, profile=profile)

    assert issue_codes(issues) == [
        "META_LABELS_TYPE",
        "META_DESCRIPTION_SOURCE_PATH_UNDETERMINED",
    ]


def test_validate_manifest_rejects_xmin_strategy_for_non_postgres_sources() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_strategy": "xmin"},
                raw_config={"source": {"type": "mssql", "table": {"schema": "dbo", "name": "orders"}}},
            ),
        )
    )

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == ["XMIN_REQUIRES_POSTGRES_SOURCE"]


def test_validate_manifest_rejects_xmin_and_incremental_column_conflict() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_strategy": "xmin", "incremental_column": "updated_at"},
                raw_config={"source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}}},
            ),
        )
    )

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == ["XMIN_CONFLICTS_WITH_INCREMENTAL_COLUMN"]


def test_validate_manifest_rejects_column_strategy_without_incremental_column() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_strategy": "column"},
                raw_config={"source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}}},
            ),
        )
    )

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == ["COLUMN_CURSOR_REQUIRES_INCREMENTAL_COLUMN"]


@pytest.mark.parametrize("strategy", ("column", "column_cursor", None))
def test_validate_manifest_rejects_explicit_and_legacy_postgres_mssql_column_cursor(
    strategy: str | None,
) -> None:
    options = {"incremental_column": "updated_at"}
    if strategy is not None:
        options["incremental_strategy"] = strategy
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options=options,
                load_strategy="incremental_merge",
                raw_config={
                    "source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}},
                    "sink": {"type": "mssql", "table": {"schema": "landing", "name": "orders"}},
                    "state": {
                        "type": "mssql",
                        "atomicity": "target_atomic",
                        "provisioning": "external",
                    },
                },
            ),
        )
    )

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == ["POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE"]
    assert "single-column MAX checkpoint" in issues[0].message
    assert "incremental_strategy=xmin" in issues[0].message


@pytest.mark.parametrize(
    ("source_type", "expected_code"),
    (
        ("mysql", "MYSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
        ("mssql", "MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
        ("sqlserver", "MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
        ("sql_server", "MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
    ),
)
@pytest.mark.parametrize("load_strategy", ("incremental_append", "incremental_merge"))
def test_validate_manifest_rejects_sibling_target_max_mssql_routes(
    source_type: str,
    expected_code: str,
    load_strategy: str,
) -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_column": "updated_at"},
                load_strategy=load_strategy,
                raw_config={
                    "source": {"type": source_type, "table": {"schema": "public", "name": "orders"}},
                    "sink": {"type": "mssql", "table": {"schema": "landing", "name": "orders"}},
                    "state": {
                        "type": "mssql",
                        "atomicity": "target_atomic",
                        "provisioning": "external",
                    },
                },
            ),
        )
    )

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == [expected_code]
    assert "strict > predicate" in issues[0].message


def test_validate_manifest_normalizes_postgresql_alias_before_xmin_policy() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_strategy": "xmin"},
                load_strategy="incremental_merge",
                raw_config={
                    "source": {"type": "postgresql", "table": {"schema": "public", "name": "orders"}},
                    "sink": {"type": "postgres", "table": {"schema": "landing", "name": "orders"}},
                },
            ),
        )
    )

    assert validate_manifest(manifest) == []


def test_validate_manifest_keeps_column_cursor_available_for_non_mssql_postgres_route() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_strategy": "column", "incremental_column": "updated_at"},
                raw_config={
                    "source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}},
                    "sink": {"type": "postgres", "table": {"schema": "landing", "name": "orders"}},
                },
            ),
        )
    )

    assert validate_manifest(manifest) == []


@pytest.mark.parametrize("incremental_column", ("event_at", None), ids=("target-max", "full-scan-fallback"))
@pytest.mark.parametrize("profile", (None, ValidationProfile(name="strict")), ids=("universal", "strict"))
def test_validate_manifest_rejects_clickhouse_mssql_incremental_append_in_every_profile(
    incremental_column: str | None,
    profile: ValidationProfile | None,
) -> None:
    options = {"incremental_column": incremental_column} if incremental_column else {}
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options=options,
                raw_config={
                    "source": {
                        "type": "clickhouse",
                        "table": {"schema": "analytics", "name": "events"},
                    },
                    "sink": {
                        "type": "mssql",
                        "table": {"schema": "landing", "name": "events"},
                        "strategy": {"mode": "incremental_append"},
                    },
                    "state": {
                        "type": "mssql",
                        "atomicity": "target_atomic",
                        "provisioning": "external",
                    },
                },
            ),
        )
    )

    issues = validate_manifest(manifest, profile=profile)

    assert issue_codes(issues) == ["CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE"]
    assert "strict > predicate" in issues[0].message
    assert "partition_replace" in issues[0].message


def test_validate_manifest_allows_clickhouse_mssql_complete_full_refresh() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                options={"incremental_column": "event_at"},
                raw_config={
                    "source": {
                        "type": "clickhouse",
                        "table": {"schema": "analytics", "name": "events"},
                    },
                    "sink": {
                        "type": "mssql",
                        "table": {"schema": "landing", "name": "events"},
                        "strategy": {"mode": "full_refresh"},
                    },
                    "state": {
                        "type": "mssql",
                        "atomicity": "target_atomic",
                        "provisioning": "external",
                    },
                },
            ),
        )
    )

    assert validate_manifest(manifest) == []


@pytest.mark.parametrize(
    ("state", "expected_code"),
    [
        (None, "MSSQL_TRANSACTION_STATE_REQUIRED"),
        ({"type": "disabled"}, "MSSQL_TRANSACTION_REQUIRES_MSSQL_STATE"),
        (
            {"type": "mssql", "atomicity": "after_target", "provisioning": "runtime"},
            "MSSQL_TRANSACTION_REQUIRES_TARGET_ATOMIC_STATE",
        ),
        (
            {"type": "mssql", "atomicity": "target_atomic", "provisioning": "runtime"},
            "MSSQL_TRANSACTION_REQUIRES_EXTERNAL_STATE",
        ),
    ],
)
def test_validate_manifest_requires_generic_mssql_governance_state(
    state: dict | None,
    expected_code: str,
) -> None:
    raw_config = {
        "source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}},
        "sink": {"type": "mssql", "table": {"schema": "dbo", "name": "orders"}},
    }
    if state is not None:
        raw_config["state"] = state
    manifest = loaded_manifest(processes=(process_spec(raw_config=raw_config),))

    issues = validate_manifest(manifest)

    assert issue_codes(issues) == [expected_code]


def test_validate_manifest_accepts_exact_generic_mssql_governance_state() -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                raw_config={
                    "source": {
                        "type": "postgres",
                        "table": {"schema": "public", "name": "orders"},
                    },
                    "sink": {
                        "type": "mssql",
                        "table": {"schema": "dbo", "name": "orders"},
                    },
                    "state": {
                        "type": "mssql",
                        "connection_ref": "mssql-governance",
                        "atomicity": "target_atomic",
                        "provisioning": "external",
                        "table": {"name": "dpone_source_state"},
                    },
                },
            ),
        )
    )

    assert validate_manifest(manifest) == []


@pytest.mark.parametrize("sink_type", _MSSQL_ALIASES)
def test_mssql_alias_cannot_bypass_generic_transaction_state(sink_type: str) -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                raw_config={
                    "source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}},
                    "sink": {"type": sink_type, "table": {"schema": "dbo", "name": "orders"}},
                }
            ),
        )
    )

    assert issue_codes(validate_manifest(manifest)) == ["MSSQL_TRANSACTION_STATE_REQUIRED"]


@pytest.mark.parametrize("state_type", _MSSQL_ALIASES)
def test_mssql_state_alias_satisfies_generic_transaction_state(state_type: str) -> None:
    manifest = loaded_manifest(
        processes=(
            process_spec(
                raw_config={
                    "source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}},
                    "sink": {"type": "odbc", "table": {"schema": "dbo", "name": "orders"}},
                    "state": {
                        "type": state_type,
                        "connection_ref": "mssql-governance",
                        "atomicity": "target_atomic",
                        "provisioning": "external",
                    },
                }
            ),
        )
    )

    assert validate_manifest(manifest) == []
