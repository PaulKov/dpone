"""Exhaustive unit matrix for the typed MSSQL batch-strategy authority."""

from __future__ import annotations

from dataclasses import replace
from itertools import product

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import (
    MSSQLStrategyContractError,
    normalize_mssql_backfill_campaign_strategy,
    normalize_mssql_load_strategy,
)


def _config(strategy: LoadStrategy, **overrides: object) -> LoadConfig:
    values: dict[str, object] = {
        "source_conn_id": "postgres",
        "target_conn_id": "mssql",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "dbo",
        "target_table": "orders",
        "load_strategy": strategy,
        "options": {"sink_type": "mssql"},
    }
    values.update(overrides)
    return LoadConfig(**values)  # type: ignore[arg-type]


def _blocked(config: LoadConfig, blocker: str) -> None:
    with pytest.raises(MSSQLStrategyContractError) as raised:
        normalize_mssql_load_strategy(config)
    assert raised.value.code == "DPONE_MSSQL_STRATEGY_CONTRACT_BLOCKED"
    assert raised.value.blocker == blocker


def test_full_refresh_and_append_finite_modes() -> None:
    for overwrite in (None, "truncate_insert"):
        policy = normalize_mssql_load_strategy(
            _config(LoadStrategy.FULL_REFRESH, overwrite_type=overwrite)
        ).full_refresh
        assert policy is not None and policy.overwrite_type == "truncate_insert"

    _blocked(
        _config(LoadStrategy.FULL_REFRESH, overwrite_type="exchange"),
        "mssql.strategy.full_refresh.exchange_physical_preservation",
    )
    _blocked(
        _config(LoadStrategy.FULL_REFRESH, overwrite_type="unknown"),
        "mssql.strategy.full_refresh.overwrite_type",
    )

    append_all = normalize_mssql_load_strategy(_config(LoadStrategy.INCREMENTAL_APPEND)).incremental_append
    assert append_all is not None and not append_all.only_new_rows and not append_all.unique_key
    append_new = normalize_mssql_load_strategy(
        _config(
            LoadStrategy.INCREMENTAL_APPEND,
            only_new_rows=True,
            unique_key=["tenant_id", "id"],
        )
    ).incremental_append
    assert append_new is not None and append_new.unique_key == ("tenant_id", "id")
    _blocked(
        _config(LoadStrategy.INCREMENTAL_APPEND, only_new_rows=True),
        "mssql.strategy.incremental_append.unique_key",
    )


def test_incremental_merge_public_policy_matrix() -> None:
    expected = {
        "auto": "delete_insert",
        "update_insert": "update_insert",
        "delete_insert": "delete_insert",
    }
    for authored, effective in expected.items():
        policy = normalize_mssql_load_strategy(
            _config(
                LoadStrategy.INCREMENTAL_MERGE,
                unique_key=["id"],
                merge_policy=authored,
            )
        ).incremental_merge
        assert policy is not None
        assert policy.merge_policy == effective
        assert policy.duplicate_policy == "fail"

    for unsupported in (
        "shadow_swap",
        "lightweight_delete_insert",
        "mutation_delete_insert",
        "event_upsert",
        "unknown",
    ):
        blocker = (
            "mssql.strategy.incremental_merge.shadow_swap_physical_preservation"
            if unsupported == "shadow_swap"
            else "mssql.strategy.incremental_merge.merge_policy"
        )
        _blocked(
            _config(
                LoadStrategy.INCREMENTAL_MERGE,
                unique_key=["id"],
                merge_policy=unsupported,
            ),
            blocker,
        )
    _blocked(
        _config(LoadStrategy.INCREMENTAL_MERGE),
        "mssql.strategy.incremental_merge.unique_key",
    )
    _blocked(
        _config(
            LoadStrategy.INCREMENTAL_MERGE,
            unique_key=["id"],
            duplicate_policy="last_write_wins",
        ),
        "mssql.strategy.incremental_merge.duplicate_policy",
    )


def test_partition_replace_all_finite_typed_submode_combinations() -> None:
    supported = 0
    rejected = 0
    for native, native_mode, require_native, values_from_staging, expression in product(
        (False, True),
        ("auto", "fallback", "required"),
        (False, True),
        (False, True),
        (None, "business_date"),
    ):
        partition = {
            "column": "business_date",
            "native": native,
            "native_mode": native_mode,
            "require_native": require_native,
            "values_from_staging": values_from_staging,
            "value_expression": expression,
            "max_partitions_per_run": 64,
        }
        config = _config(LoadStrategy.PARTITION_REPLACE, partition=partition)
        executable = native_mode != "required" and not require_native and values_from_staging and expression is None
        if executable:
            policy = normalize_mssql_load_strategy(config).partition_replace
            assert policy is not None
            assert policy.native_mode == ("auto" if native and native_mode == "auto" else "fallback")
            assert policy.native is (native and native_mode == "auto")
            supported += 1
        else:
            with pytest.raises(MSSQLStrategyContractError):
                normalize_mssql_load_strategy(config)
            rejected += 1

    assert (supported, rejected) == (4, 44)

    base = _config(
        LoadStrategy.PARTITION_REPLACE,
        partition={"column": "business_date", "native_mode": "fallback"},
    )
    for maximum in (1, 64, 4096):
        policy = normalize_mssql_load_strategy(
            replace(base, partition={**base.partition, "max_partitions_per_run": maximum})
        ).partition_replace
        assert policy is not None and policy.max_partitions_per_run == maximum
    for maximum in (0, -1, "invalid"):
        _blocked(
            replace(base, partition={**base.partition, "max_partitions_per_run": maximum}),
            "mssql.strategy.partition_replace.max_partitions_per_run",
        )


def test_snapshot_diff_compare_by_delete_policy_cartesian_product() -> None:
    seen: set[tuple[str, str]] = set()
    for compare, delete_policy in product(
        ("row_hash", "all_columns"),
        ("ignore", "hard_delete", "soft_delete"),
    ):
        config = _config(
            LoadStrategy.SNAPSHOT_DIFF,
            unique_key=["id"],
            options={
                "sink_type": "mssql",
                "diff": {"compare": compare, "delete_policy": delete_policy},
            },
        )
        policy = normalize_mssql_load_strategy(config).snapshot_diff
        assert policy is not None
        seen.add((policy.compare, policy.delete_policy))
    assert seen == set(product(("row_hash", "all_columns"), ("ignore", "hard_delete", "soft_delete")))


@pytest.mark.parametrize("scope_field", ("options", "custom_predicate"))
@pytest.mark.parametrize("delete_policy", ("hard_delete", "soft_delete"))
def test_destructive_snapshot_rejects_unbound_source_scope(
    scope_field: str,
    delete_policy: str,
) -> None:
    options = {
        "sink_type": "mssql",
        "diff": {"delete_policy": delete_policy},
    }
    overrides: dict[str, object] = {"options": options}
    if scope_field == "options":
        options["source_custom_predicate"] = "tenant_id = 7"
    else:
        overrides["custom_predicate"] = "tenant_id = 7"
    _blocked(
        _config(LoadStrategy.SNAPSHOT_DIFF, unique_key=["id"], **overrides),
        "mssql.strategy.snapshot_diff.source_custom_predicate",
    )


@pytest.mark.parametrize("scope_field", ("options", "custom_predicate"))
def test_destructive_scd2_rejects_unbound_source_scope(scope_field: str) -> None:
    options = {"sink_type": "mssql", "scd2": {"delete_policy": "expire"}}
    overrides: dict[str, object] = {"options": options}
    if scope_field == "options":
        options["source_custom_predicate"] = "tenant_id = 7"
    else:
        overrides["custom_predicate"] = "tenant_id = 7"
    _blocked(
        _config(LoadStrategy.SCD2, unique_key=["id"], **overrides),
        "mssql.strategy.scd2.source_custom_predicate",
    )


@pytest.mark.parametrize(
    ("strategy", "options"),
    (
        (LoadStrategy.SNAPSHOT_DIFF, {"diff": {"delete_policy": "ignore"}}),
        (LoadStrategy.SCD2, {"scd2": {"delete_policy": "ignore"}}),
    ),
)
def test_non_destructive_snapshot_modes_allow_source_scope(strategy: LoadStrategy, options: dict) -> None:
    config = _config(
        strategy,
        unique_key=["id"],
        options={"sink_type": "mssql", "source_custom_predicate": "tenant_id = 7", **options},
    )
    normalize_mssql_load_strategy(config)


def test_scd2_and_backfill_public_submodes() -> None:
    for delete_policy in ("expire", "ignore"):
        policy = normalize_mssql_load_strategy(
            _config(
                LoadStrategy.SCD2,
                unique_key=["id"],
                options={"sink_type": "mssql", "scd2": {"delete_policy": delete_policy}},
            )
        ).scd2
        assert policy is not None and policy.delete_policy == delete_policy
    _blocked(
        _config(
            LoadStrategy.SCD2,
            unique_key=["id"],
            options={
                "sink_type": "mssql",
                "scd2": {"delete_policy": "hard_delete_not_supported"},
            },
        ),
        "mssql.strategy.scd2.delete_policy",
    )

    backfills = (
        _config(
            LoadStrategy.BACKFILL,
            partition={"column": "business_date", "native_mode": "fallback"},
            options={
                "sink_type": "mssql",
                "backfill": {"inner_mode": "partition_replace", "parallel_workers": 2},
            },
        ),
        _config(
            LoadStrategy.BACKFILL,
            custom_predicate="business_date = '2026-08-15'",
            options={
                "sink_type": "mssql",
                "backfill": {"inner_mode": "replace", "parallel_workers": 1},
            },
        ),
        _config(
            LoadStrategy.BACKFILL,
            unique_key=["id"],
            options={
                "sink_type": "mssql",
                "backfill": {"inner_mode": "incremental_merge", "parallel_workers": 2},
            },
        ),
    )
    assert [normalize_mssql_load_strategy(config).backfill.inner_mode for config in backfills] == [
        "partition_replace",
        "replace",
        "incremental_merge",
    ]
    _blocked(
        replace(
            backfills[0],
            options={"sink_type": "mssql", "backfill": {"inner_mode": "full_refresh"}},
        ),
        "mssql.strategy.backfill.inner_mode",
    )


def _shadow_backfill(**overrides: object) -> LoadConfig:
    backfill: dict[str, object] = {
        "inner_mode": "incremental_append",
        "parallel_workers": 4,
        "chunk": {"column": "id", "kind": "uuid", "buckets": 512},
        "max_chunks": 512,
        "state": {
            "backend": "audit_schema",
            "schema": "system",
            "require_distributed_lock": True,
        },
        "publication": {
            "mode": "shadow_swap",
            "retain_backup": True,
            "artifact_scope": "campaign",
        },
    }
    options: dict[str, object] = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "backfill": backfill,
    }
    config_overrides: dict[str, object] = {"unique_key": ["id"], "options": options}
    config_overrides.update(overrides)
    return _config(LoadStrategy.BACKFILL, **config_overrides)


def test_shadow_initial_backfill_contract_is_explicit_and_fail_closed() -> None:
    policy = normalize_mssql_load_strategy(_shadow_backfill()).backfill

    assert policy is not None
    assert policy.inner_mode == "incremental_append"
    assert policy.publication_mode == "shadow_swap"
    assert policy.retain_backup is True
    assert policy.inner_policy is not None
    assert policy.inner_policy.unique_key == ("id",)

    cases = (
        (
            {
                "options": {
                    "source_type": "postgres",
                    "sink_type": "mssql",
                    "backfill": {
                        "inner_mode": "incremental_append",
                        "chunk": {"column": "id", "kind": "uuid", "buckets": 4},
                        "state": {"backend": "audit_schema", "require_distributed_lock": True},
                    },
                }
            },
            "mssql.strategy.backfill.incremental_append_publication",
        ),
        ({"only_new_rows": True}, "mssql.strategy.backfill.shadow_swap_only_new_rows"),
        ({"unique_key": None}, "mssql.strategy.backfill.shadow_swap_unique_key"),
    )
    for overrides, blocker in cases:
        _blocked(_shadow_backfill(**overrides), blocker)


def test_shadow_initial_rejects_unsafe_state_route_and_inner_mode() -> None:
    config = _shadow_backfill()
    config.options["backfill"]["state"] = {  # type: ignore[index]
        "backend": "audit_schema",
        "schema": "system",
        "require_distributed_lock": False,
    }
    _blocked(config, "mssql.strategy.backfill.shadow_swap_state")

    config = _shadow_backfill()
    config.options["source_type"] = "mysql"
    _blocked(config, "mssql.strategy.backfill.shadow_swap_route")

    config = _shadow_backfill()
    config.options["backfill"]["inner_mode"] = "incremental_merge"  # type: ignore[index]
    _blocked(config, "mssql.strategy.backfill.shadow_swap_inner_mode")


def test_scd2_physical_primary_key_requires_history_discriminator() -> None:
    base = {
        "sink_type": "mssql",
        "scd2": {"delete_policy": "ignore"},
    }
    _blocked(
        _config(
            LoadStrategy.SCD2,
            unique_key=["tenant_id", "id"],
            options={
                **base,
                "physical_design": {
                    "indexes": {"primary_key": ["tenant_id", "id"]},
                },
            },
        ),
        "mssql.strategy.scd2.physical_primary_key",
    )

    policy = normalize_mssql_load_strategy(
        _config(
            LoadStrategy.SCD2,
            unique_key=["tenant_id", "id"],
            options={
                **base,
                "physical_design": {
                    "indexes": {
                        "primary_key": [
                            "tenant_id",
                            "id",
                            "__dpone__valid_from_at",
                        ]
                    },
                },
            },
        )
    ).scd2
    assert policy is not None


def test_replace_cdc_and_irrelevant_sections_fail_closed() -> None:
    normalize_mssql_load_strategy(_config(LoadStrategy.REPLACE, custom_predicate="business_date = '2026-08-15'"))
    _blocked(
        _config(LoadStrategy.REPLACE),
        "mssql.strategy.replace.custom_predicate",
    )
    _blocked(
        _config(LoadStrategy.CDC_APPLY, unique_key=["id"]),
        "mssql.strategy.mode",
    )
    _blocked(
        _config(
            LoadStrategy.FULL_REFRESH,
            options={"sink_type": "mssql", "diff": {"compare": "row_hash"}},
        ),
        "mssql.strategy.full_refresh.irrelevant_diff",
    )


@pytest.mark.parametrize("source_type", ("postgres", "postgresql", "PostgreSQL"))
def test_cross_dialect_raw_replace_scope_fails_before_source_io(source_type: str) -> None:
    _blocked(
        _config(
            LoadStrategy.REPLACE,
            custom_predicate="business_date = DATE '2026-08-15'",
            options={"source_type": source_type, "sink_type": "mssql"},
        ),
        "mssql.strategy.replace.cross_dialect_raw_predicate",
    )
    _blocked(
        _config(
            LoadStrategy.BACKFILL,
            custom_predicate="business_date = DATE '2026-08-15'",
            options={
                "source_type": source_type,
                "sink_type": "mssql",
                "backfill": {"inner_mode": "replace", "parallel_workers": 1},
            },
        ),
        "mssql.strategy.backfill.replace_cross_dialect_raw_predicate",
    )


def test_campaign_normalization_defers_only_the_planner_owned_replace_scope() -> None:
    config = _config(
        LoadStrategy.BACKFILL,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "backfill": {
                "inner_mode": "replace",
                "chunk": {
                    "column": "id",
                    "kind": "integer",
                    "from": "1",
                    "to": "2",
                    "step": "1",
                },
            },
        },
    )

    policy = normalize_mssql_backfill_campaign_strategy(config).backfill
    assert policy is not None and policy.inner_mode == "replace"
    _blocked(config, "mssql.strategy.backfill.scope_required")

    with pytest.raises(MSSQLStrategyContractError) as raised:
        normalize_mssql_backfill_campaign_strategy(replace(config, only_new_rows=True))
    assert raised.value.blocker == "mssql.strategy.backfill.irrelevant_only_new_rows"


def test_campaign_normalization_requires_scope_when_no_chunk_planner_exists() -> None:
    config = _config(
        LoadStrategy.BACKFILL,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "backfill": {"inner_mode": "replace"},
        },
    )

    with pytest.raises(MSSQLStrategyContractError) as raised:
        normalize_mssql_backfill_campaign_strategy(config)
    assert raised.value.blocker == "mssql.strategy.backfill.scope_required"


def test_legacy_unchunked_backfill_accepts_one_explicit_portable_scope() -> None:
    config = _config(
        LoadStrategy.BACKFILL,
        portable_scope={
            "version": 1,
            "kind": "equality",
            "column": "business_date",
            "value": {"type": "date", "value": "2026-08-23"},
        },
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "backfill": {"inner_mode": "replace"},
        },
    )

    assert normalize_mssql_backfill_campaign_strategy(config).backfill is not None
    assert normalize_mssql_load_strategy(config).backfill is not None


def test_chunked_backfill_rejects_authored_portable_scope_before_planning() -> None:
    config = _config(
        LoadStrategy.BACKFILL,
        portable_scope={
            "version": 1,
            "kind": "equality",
            "column": "business_date",
            "value": {"type": "date", "value": "2026-08-23"},
        },
        options={
            "source_type": "postgresql",
            "sink_type": "sql-server",
            "backfill": {
                "inner_mode": "replace",
                "chunk": {
                    "column": "id",
                    "kind": "integer",
                    "from": "1",
                    "to": "2",
                    "step": "1",
                },
            },
        },
    )

    with pytest.raises(MSSQLStrategyContractError) as raised:
        normalize_mssql_backfill_campaign_strategy(config)

    assert raised.value.blocker == "backfill.portable_scope_is_runtime_owned"


def test_conflicting_source_scope_declarations_fail_closed() -> None:
    _blocked(
        _config(
            LoadStrategy.REPLACE,
            custom_predicate="tenant_id = 7",
            options={
                "sink_type": "mssql",
                "source_custom_predicate": "tenant_id = 8",
            },
        ),
        "mssql.strategy.replace.source_scope_ambiguous",
    )
