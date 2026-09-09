"""Real-vendor registry and binary target-authority certification.

This module deliberately stops at the registry/state boundary.  It reuses the
disposable PostgreSQL 16 and SQL Server 2022 route fixture, but does not run
payload DML or write certification artifacts.  The caller owns orchestration
and publication of the sanitized evidence returned by
``run_target_identity_registry_live``.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.ports.source_state_storage import SourceStateKey
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_options import (
    target_lock_resource,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    CASE_SENSITIVE_COLLATION as _CASE_SENSITIVE_COLLATION,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    active_owners as _active_owners,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    case_sensitive_target as _case_sensitive_target,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    commit_owner as _commit_owner,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    config as _config,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    database_collation as _database_collation,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    pre_source_failure as _pre_source_failure,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    receipt_count as _receipt_count,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    require_collation_mode as _require_collation_mode,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    set_process as _set_process,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    state_key as _state_key,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    strategy as _strategy,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry_support import (
    target_coordinates as _target_coordinates,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_matrix_support import (
    fork_snapshot_route,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    TARGET_SCHEMA,
    TARGET_TABLE,
    provision_snapshot_route,
)


def run_target_identity_registry_live(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    """Run the real registry/state matrix and return secret-free evidence."""

    root.mkdir(parents=True, exist_ok=True)
    ci_alias = _ci_alias_convergence(root / "ci-alias", route_live_recorder)
    cs_distinct, pre_source = _cs_distinct_and_pre_source_failures(
        root / "cs-distinct",
        route_live_recorder,
    )
    concurrent = _concurrent_distinct_owners(
        root / "concurrent-owners",
        route_live_recorder,
    )
    cases = {
        "ci_alias_convergence": ci_alias,
        "cs_case_distinct_mixed_collation": cs_distinct,
        "missing_and_decoy_pre_source": pre_source,
        "concurrent_distinct_owners": concurrent,
    }
    if not all(case.get("passed") is True for case in cases.values()):
        raise AssertionError("target identity registry live matrix is incomplete")
    return {
        "schema_version": "dpone.postgres_mssql.target_identity_live.v1",
        "status": "passed",
        "cases": cases,
    }


def _ci_alias_convergence(
    work_dir: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    with provision_snapshot_route(work_dir) as route:
        before = _registry_state_image(route)
        target_collation = _database_collation(route.target)
        state_collation = _database_collation(route.state)
        _require_collation_mode(target_collation, "CI")
        _require_collation_mode(state_collation, "CI")
        strategy = route.processor.source._xmin_extract
        config = _config(
            route.load_config,
            database=route.target_database.upper(),
            schema=TARGET_SCHEMA.upper(),
            table=TARGET_TABLE.upper(),
            process="integration.target_identity.ci_alias",
        )

        strategy._preflight_atomic_route(config)
        first_identity = strategy.physical_target_identity(config)
        first_coordinates = _target_coordinates(config)

        config.target_database = route.target_database.swapcase()
        config.target_schema = "SaMpLe_MeTrIcS"
        config.target_table = "MeTrIc_VaLuEs"
        strategy._preflight_atomic_route(config)
        second_identity = strategy.physical_target_identity(config)
        second_coordinates = _target_coordinates(config)

        expected = (route.target_database, TARGET_SCHEMA, TARGET_TABLE)
        assert first_identity == second_identity
        assert first_coordinates == second_coordinates == expected
        result = {
            "passed": True,
            "target_collation": target_collation,
            "state_collation": state_collation,
            "aliases_converged": True,
            "canonical_schema": second_coordinates[1],
            "canonical_table": second_coordinates[2],
            "target_identity_hex": first_identity.hex(),
        }
        after = _registry_state_image(route)
        assert after == before
        route_live_recorder.observe_case(
            "target_identity_authority",
            "target_identity_ci_alias_convergence",
            before_image=before,
            after_image=after,
            observations=result,
        )
        return result


def _cs_distinct_and_pre_source_failures(
    work_dir: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> tuple[dict[str, Any], dict[str, Any]]:
    with provision_snapshot_route(work_dir) as route:
        state_collation = _database_collation(route.state)
        _require_collation_mode(state_collation, "CI")
        with _case_sensitive_target(route) as (target, database, bindings):
            before_distinct = _registry_state_image(route)
            target_collation = _database_collation(target)
            assert target_collation == _CASE_SENSITIVE_COLLATION
            process = "integration.target_identity.cs_case_distinct"
            upper_strategy, upper_state, upper_config = _strategy(
                route,
                target=target,
                database=database,
                table="Foo",
                process=process,
            )
            lower_strategy, lower_state, lower_config = _strategy(
                route,
                target=target,
                database=database,
                table="foo",
                process=process,
            )
            upper_strategy._preflight_atomic_route(upper_config)
            lower_strategy._preflight_atomic_route(lower_config)
            upper_identity = upper_strategy.physical_target_identity(upper_config)
            lower_identity = lower_strategy.physical_target_identity(lower_config)
            assert upper_identity != lower_identity
            assert upper_config.target_table == "Foo"
            assert lower_config.target_table == "foo"

            upper_key = _state_key(upper_strategy, upper_config)
            lower_key = _state_key(lower_strategy, lower_config)
            assert upper_key.digest != lower_key.digest
            assert upper_key.target_identity != lower_key.target_identity
            upper_xmin = upper_strategy.xmin_manager.get_snapshot_xmin_anchor()
            lower_xmin = lower_strategy.xmin_manager.get_snapshot_xmin_anchor()
            _commit_owner(target, upper_state, upper_key, upper_xmin, "cs-upper")
            _commit_owner(target, lower_state, lower_key, lower_xmin, "cs-lower")

            active = _active_owners(route)
            receipts = _receipt_count(route)
            assert len(active) == receipts == 2
            assert {bytes(row["target_identity"]) for row in active} == {upper_identity, lower_identity}
            assert {str(row["target_table"]) for row in active} == {"Foo", "foo"}
            distinct_result = {
                "passed": True,
                "target_collation": target_collation,
                "state_collation": state_collation,
                "case_distinct": True,
                "binding_ids": {name: str(binding) for name, binding in bindings.items()},
                "target_identity_hex": {
                    "Foo": upper_identity.hex(),
                    "foo": lower_identity.hex(),
                },
                "state_key_hex": {"Foo": upper_key.digest.hex(), "foo": lower_key.digest.hex()},
                "active_owner_count": len(active),
                "receipt_count": receipts,
                "active_target_spellings": sorted(str(row["target_table"]) for row in active),
            }
            after_distinct = _registry_state_image(route)
            route_live_recorder.observe_case(
                "target_identity_authority",
                "target_identity_cs_case_distinct_mixed_collation",
                before_image=before_distinct,
                after_image=after_distinct,
                observations=distinct_result,
            )

            before_pre_source = _registry_state_image(route)
            missing = _pre_source_failure(
                route,
                target=target,
                database=database,
                table="Missing",
                expected="binding_missing_or_ambiguous",
            )
            target.execute_query(
                f"ALTER TABLE [{database}].[dbo].[dpone_target_identity] ADD [Binding_ID] uniqueidentifier NULL"
            )
            decoy = _pre_source_failure(
                route,
                target=target,
                database=database,
                table="Foo",
                expected="identifier_case_ambiguity",
            )
            pre_source_result = {
                "passed": True,
                "target_collation": target_collation,
                "missing": missing,
                "case_variant_decoy": decoy,
            }
            after_pre_source = _registry_state_image(route)
            assert after_pre_source == before_pre_source
            route_live_recorder.observe_case(
                "target_identity_authority",
                "target_identity_missing_decoy_pre_source",
                before_image=before_pre_source,
                after_image=after_pre_source,
                observations={
                    **pre_source_result,
                    "decoy_catalog_change_is_test_arrangement": True,
                },
            )

            return distinct_result, pre_source_result


def _concurrent_distinct_owners(
    work_dir: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    with provision_snapshot_route(work_dir) as route:
        before = _registry_state_image(route)
        with (
            fork_snapshot_route(route, work_dir / "owner-a") as first,
            fork_snapshot_route(route, work_dir / "owner-b") as second,
        ):
            first_strategy = first.processor.source._xmin_extract
            second_strategy = second.processor.source._xmin_extract
            _set_process(first.load_config, "integration.target_identity.concurrent.a")
            _set_process(second.load_config, "integration.target_identity.concurrent.b")
            first_strategy._preflight_atomic_route(first.load_config)
            second_strategy._preflight_atomic_route(second.load_config)
            first_key = _state_key(first_strategy, first.load_config)
            second_key = _state_key(second_strategy, second.load_config)
            assert first_key.target_identity == second_key.target_identity
            assert first_key.digest != second_key.digest
            resource = target_lock_resource(first_key.target_identity)
            assert resource == target_lock_resource(second_key.target_identity)
            first_xmin = first_strategy.xmin_manager.get_snapshot_xmin_anchor()
            second_xmin = second_strategy.xmin_manager.get_snapshot_xmin_anchor()
            barrier = threading.Barrier(2)

            def attempt(environment: Any, key: SourceStateKey, xmin: int, label: str) -> dict[str, Any]:
                barrier.wait(timeout=60)
                return _commit_owner(
                    environment.target,
                    environment.processor.source._xmin_extract.state_storage,
                    key,
                    xmin,
                    label,
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = (
                    pool.submit(attempt, first, first_key, first_xmin, "concurrent-a"),
                    pool.submit(attempt, second, second_key, second_xmin, "concurrent-b"),
                )
                outcomes: list[dict[str, Any] | BaseException] = []
                for future in futures:
                    try:
                        outcomes.append(future.result(timeout=120))
                    except BaseException as exc:  # noqa: BLE001 - exact loser diagnostic is asserted below.
                        outcomes.append(exc)

        successes = [outcome for outcome in outcomes if isinstance(outcome, dict)]
        failures = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
        assert len(successes) == len(failures) == 1
        assert "DPONE_XMIN_TARGET_AUTHORITY_CONFLICT" in str(failures[0])
        active = _active_owners(route)
        receipts = _receipt_count(route)
        assert len(active) == receipts == 1
        assert bytes(active[0]["target_identity"]) == first_key.target_identity
        assert bytes(active[0]["state_key"]) in {first_key.digest, second_key.digest}
        result = {
            "passed": True,
            "target_collation": _database_collation(route.target),
            "state_collation": _database_collation(route.state),
            "physical_identity_hex": first_key.target_identity.hex(),
            "applock_resource": resource,
            "distinct_state_keys": sorted((first_key.digest.hex(), second_key.digest.hex())),
            "committed_attempts": len(successes),
            "rejected_attempts": len(failures),
            "rejection": "DPONE_XMIN_TARGET_AUTHORITY_CONFLICT",
            "active_owner_count": len(active),
            "receipt_count": receipts,
        }
        route_live_recorder.observe_case(
            "target_identity_authority",
            "target_identity_concurrent_distinct_owners",
            before_image=before,
            after_image=_registry_state_image(route),
            observations=result,
        )
        return result


def _registry_state_image(route: Any) -> dict[str, object]:
    owners = sorted(
        (
            {
                "target_identity": bytes(row["target_identity"]),
                "state_key": bytes(row["state_key"]),
                "target_table": str(row["target_table"]),
            }
            for row in _active_owners(route)
        ),
        key=lambda row: (row["target_identity"], row["state_key"]),
    )
    return {
        "active_owners": owners,
        "receipt_count": _receipt_count(route),
    }


__all__ = ["run_target_identity_registry_live"]
