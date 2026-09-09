from __future__ import annotations

from datetime import date

import pytest

from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.readiness.schema_identity import (
    AliasProjectionPlanner,
    SchemaIdentityOptions,
    SchemaIdentityResolver,
)


def test_disabled_identity_preserves_current_drop_add_rename_behavior() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[ColumnDef("customer_id", "bigint")],
        target=[ColumnDef("client_id", "bigint")],
    )

    assert [change.change_type for change in plan.changes] == ["add_column", "drop_column"]
    assert plan.has_breaking_changes is True


def test_explicit_alias_canonicalizes_rename_before_schema_evolution() -> None:
    options = SchemaIdentityOptions.from_config(
        {
            "enabled": True,
            "columns": {
                "customer_id": {
                    "id": "orders.customer_id",
                    "aliases": [{"name": "client_id", "remove_after": "2026-09-01"}],
                }
            },
        }
    )

    result = SchemaIdentityResolver(options).resolve(
        source=[ColumnDef("client_id", "bigint")],
        target=[ColumnDef("customer_id", "bigint")],
        today=date(2026, 6, 21),
    )
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=list(result.canonical_source),
        target=[ColumnDef("customer_id", "bigint")],
    )

    assert result.blockers == ()
    assert result.decisions[0].action == "rename_alias"
    assert result.decisions[0].observed_name == "client_id"
    assert result.decisions[0].canonical_name == "customer_id"
    assert plan.changes == []


def test_schema_identity_rejects_duplicate_ids_and_alias_collisions() -> None:
    with pytest.raises(ValueError, match="duplicate identity id"):
        SchemaIdentityOptions.from_config(
            {
                "enabled": True,
                "columns": {
                    "customer_id": {"id": "orders.party"},
                    "buyer_id": {"id": "orders.party"},
                },
            }
        )

    with pytest.raises(ValueError, match="duplicate alias"):
        SchemaIdentityOptions.from_config(
            {
                "enabled": True,
                "columns": {
                    "customer_id": {"id": "orders.customer", "aliases": [{"name": "party_id"}]},
                    "buyer_id": {"id": "orders.buyer", "aliases": [{"name": "party_id"}]},
                },
            }
        )


def test_expired_alias_blocks_or_warns_by_policy() -> None:
    blocking = SchemaIdentityOptions.from_config(
        {
            "enabled": True,
            "expired_alias": "block",
            "columns": {
                "customer_id": {
                    "id": "orders.customer_id",
                    "aliases": [{"name": "client_id", "remove_after": "2026-01-01"}],
                }
            },
        }
    )
    warning = SchemaIdentityOptions.from_config(
        {
            "enabled": True,
            "expired_alias": "warn",
            "columns": {
                "customer_id": {
                    "id": "orders.customer_id",
                    "aliases": [{"name": "client_id", "remove_after": "2026-01-01"}],
                }
            },
        }
    )

    blocked = SchemaIdentityResolver(blocking).resolve(
        source=[ColumnDef("client_id", "bigint")],
        today=date(2026, 6, 21),
    )
    warned = SchemaIdentityResolver(warning).resolve(
        source=[ColumnDef("client_id", "bigint")],
        today=date(2026, 6, 21),
    )

    assert blocked.blockers == ("schema_identity.alias_expired:customer_id:client_id",)
    assert warned.blockers == ()
    assert warned.warnings == ("schema_identity.alias_expired:customer_id:client_id",)


def test_alias_projection_blocks_conflicts_and_supports_dual_write() -> None:
    options = SchemaIdentityOptions.from_config(
        {
            "enabled": True,
            "columns": {
                "customer_id": {
                    "id": "orders.customer_id",
                    "aliases": [{"name": "client_id", "compatibility": "dual_write"}],
                }
            },
        }
    )
    planner = AliasProjectionPlanner(options)

    equal = planner.project_rows(
        rows=[{"customer_id": 7, "client_id": 7, "status": "ok"}],
        schema=[("client_id", "bigint"), ("customer_id", "bigint"), ("status", "text")],
    )
    conflict = planner.project_rows(
        rows=[{"customer_id": 7, "client_id": 8}],
        schema=[("client_id", "bigint"), ("customer_id", "bigint")],
    )

    assert equal.blockers == ()
    assert equal.rows == ({"customer_id": 7, "client_id": 7, "status": "ok"},)
    assert ("client_id", "bigint") in equal.schema
    assert conflict.blockers == ("schema_identity.alias_value_conflict:customer_id:client_id",)


def test_rename_plus_type_change_generates_variant_column_from_canonical_name() -> None:
    options = SchemaIdentityOptions.from_config(
        {
            "enabled": True,
            "columns": {
                "customer_id": {
                    "id": "orders.customer_id",
                    "aliases": [{"name": "client_id"}],
                }
            },
        }
    )
    identity = SchemaIdentityResolver(options).resolve(
        source=[ColumnDef("client_id", "nvarchar(100)")],
        target=[ColumnDef("customer_id", "int")],
    )
    plan = SchemaComparator(SchemaEvolutionPolicy(on_type_change="new_column")).compare(
        source=list(identity.canonical_source),
        target=[ColumnDef("customer_id", "int")],
    )

    assert plan.column_mapping == {"customer_id": "__dpone__nc__customer_id"}
    assert "__dpone__nc__client_id" not in plan.to_dict()["generated_columns"].values()
