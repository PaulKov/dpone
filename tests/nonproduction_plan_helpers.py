"""Declared originals for codec tests; no physical or authority observations."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from typing import Any, Literal, cast

from dpone.contracts.composition_ownership import CompositionPhysicalClaim
from dpone.contracts.composition_qualification_operation import (
    CompositionQualificationOperation,
    CompositionQualificationOwner,
)
from dpone.contracts.nonproduction_authority import NonproductionLimits
from dpone.contracts.nonproduction_grants import NonproductionQualificationGrant
from dpone.contracts.nonproduction_scope import NonproductionParticipant, canonical_document
from tests.nonproduction_authority_helpers import qualification, scope

BCP = "mssql_clickhouse_bcp_wide_v1"
PG = "postgres_mssql_wide_v1"
SEQUENCES = ("c_bigserial_sequence", "c_identity_sequence", "c_serial_sequence", "c_smallserial_sequence")


def sha(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def label_sha(label: str) -> str:
    return sha(label.encode())


def limits(**changes: int) -> dict[str, int]:
    return asdict(replace(NonproductionLimits(86400, 64, 128, 100000, 1073741824, 3600), **changes))


def descriptor(path: str = "recipes/input.json") -> dict[str, Any]:
    return {"path": path, "sha256": label_sha(path), "size_bytes": 100}


def declared(object_id: str, connector: str, kind: str = "table") -> dict[str, Any]:
    number = {"postgres": 1, "mssql": 2, "clickhouse": 3}[connector]
    return {
        "object_id": object_id,
        "connector": connector,
        "service_id": f"10000000-0000-4000-8000-{number:012d}",
        "physical_subject_sha256": label_sha(connector),
        "subject_sha256": label_sha(object_id),
        "object_kind": kind,
        "qualified_name": ["database", "schema", object_id] if connector != "clickhouse" else ["database", object_id],
    }


def effect(object_id: str, access: str, purpose: str = "fixture") -> dict[str, str]:
    return {"object_id": object_id, "access": access, "purpose": purpose}


def item(item_id: str, fixture: str, action: str, **extra: Any) -> dict[str, Any]:
    return {
        "work_item_id": item_id,
        "fixture_id": fixture,
        "action": action,
        "predecessor_ids": [],
        "effects": [],
        "limits": limits(),
        **extra,
    }


def route(item_id: str, fixture: str, source: str, target: str, predecessor: str) -> dict[str, Any]:
    dimensions = asdict(scope().routes[0 if fixture == "bcp" else 1])
    return item(
        item_id,
        fixture,
        "route_qualification",
        predecessor_ids=[predecessor],
        effects=[effect(source, "read", "source"), effect(target, "write", "target")],
        route=dimensions,
        reviewed_case_original=descriptor("cases/route.json"),
        source_bound={
            "source_object_id": source,
            "accounting_profile": "mssql_bcp_native_file" if fixture == "bcp" else "postgres_copy_payload",
            "input_original": descriptor(),
            "projection_original": descriptor("recipes/projection.json"),
            "derivation_original": descriptor("recipes/derivation.json"),
        },
    )


def documents(*, watermark: bool = False, seals: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    objects = [
        declared("bcp_source", "mssql"),
        declared("bcp_target", "clickhouse"),
        declared("pg_source", "postgres"),
        declared("pg_target", "mssql"),
        declared("enum", "postgres", "enum"),
    ]
    objects += [declared(name, "postgres", "sequence") for name in SEQUENCES]
    fixtures = [
        {
            "fixture_id": "bcp",
            "profile": BCP,
            "recipe_original": descriptor("tools/mssql_clickhouse_bcp_native_fixtures.py"),
            "parameters": {"row_count": 10},
            "bindings": {"source": "bcp_source", "target": "bcp_target"},
        },
        {
            "fixture_id": "pg",
            "profile": PG,
            "recipe_original": descriptor("tests/integration/postgres/postgres_mssql_wide_fixtures.py"),
            "parameters": {"watermark_key_3": watermark},
            "bindings": {
                "source": "pg_source",
                "target": "pg_target",
                "enum": "enum",
                **{key: key for key in SEQUENCES},
            },
        },
    ]
    items = [
        item("bcp_initial", "bcp", "fixture_seed", seed_step="initial", effects=[effect("bcp_source", "write")]),
        route("bcp_route", "bcp", "bcp_source", "bcp_target", "bcp_initial"),
        item(
            "pg_initial",
            "pg",
            "fixture_seed",
            seed_step="initial",
            effects=[
                effect("pg_source", "write"),
                effect("enum", "read", "helper"),
                effect("enum", "write", "helper"),
                *[effect(key, "write", "helper") for key in SEQUENCES],
            ],
        ),
        route("pg_route", "pg", "pg_source", "pg_target", "pg_initial"),
    ]
    if watermark:
        items.append(
            item(
                "pg_watermark",
                "pg",
                "fixture_seed",
                seed_step="watermark_3",
                predecessor_ids=["pg_route"],
                effects=[effect("pg_source", "write"), effect("enum", "read", "helper")],
            )
        )
        items.append(route("pg_route_final", "pg", "pg_source", "pg_target", "pg_watermark"))
    if seals:
        for fixture in ("bcp", "pg"):
            predecessor = "pg_route_final" if fixture == "pg" and watermark else fixture + "_route"
            items.append(
                item(
                    fixture + "_seal",
                    fixture,
                    "source_seal",
                    predecessor_ids=[predecessor],
                    retained_source_ids=[fixture + "_source"],
                    seal_recipe_original=descriptor("recipes/seal.json"),
                    effects=[effect(fixture + "_source", "read", "source")],
                )
            )
    fixture_doc = {
        "schema": "dpone.nonproduction-fixture-plan.v1",
        "scope_sha256": label_sha("pending"),
        "objects": objects,
        "fixtures": fixtures,
    }
    plan_doc = {
        "schema": "dpone.nonproduction-qualification-plan.v1",
        "scope_sha256": label_sha("pending"),
        "fixture_plan_sha256": label_sha("pending"),
        "work_items": items,
    }
    order_documents(fixture_doc, plan_doc)
    return fixture_doc, plan_doc


def order_documents(fixture: dict[str, Any], plan: dict[str, Any]) -> None:
    fixture["objects"].sort(key=lambda row: row["object_id"])
    fixture["fixtures"].sort(key=lambda row: row["fixture_id"])
    plan["work_items"].sort(key=lambda row: row["work_item_id"])
    for row in plan["work_items"]:
        row["effects"].sort(key=lambda edge: (edge["object_id"], edge["access"], edge["purpose"]))
        row["predecessor_ids"].sort()


def find_item(plan: dict[str, Any], name: str) -> dict[str, Any]:
    return next(row for row in plan["work_items"] if row["work_item_id"] == name)


def originals(fixture: dict[str, Any], plan: dict[str, Any]) -> tuple[bytes, bytes, NonproductionQualificationGrant]:
    objects = {row["object_id"]: row for row in fixture["objects"]}
    union: dict[tuple[str, str, str], dict[str, set[str]]] = {}
    for row in plan["work_items"]:
        for edge in row["effects"]:
            obj = objects[edge["object_id"]]
            domain = (obj["connector"], obj["service_id"], obj["physical_subject_sha256"])
            directions = union.setdefault(domain, {"read": set(), "write": set()})
            directions[edge["access"]].add(obj["subject_sha256"])
    participants = tuple(
        sorted(
            NonproductionParticipant(*key, tuple(sorted(value["read"])), tuple(sorted(value["write"])))
            for key, value in union.items()
        )
    )
    subject = scope(participants=participants)
    fixture["scope_sha256"] = plan["scope_sha256"] = subject.scope_sha256
    fixture_bytes = canonical_document(fixture)
    plan["fixture_plan_sha256"] = sha(fixture_bytes)
    plan_bytes = canonical_document(plan)
    grant = qualification(
        scope=subject, fixture_plan_sha256=sha(fixture_bytes), qualification_plan_sha256=sha(plan_bytes)
    )
    assert type(grant) is NonproductionQualificationGrant
    return fixture_bytes, plan_bytes, grant


def owner_operation(
    grant: NonproductionQualificationGrant, row: dict[str, Any], fixture: dict[str, Any]
) -> tuple[CompositionQualificationOwner, CompositionQualificationOperation]:
    objects = {obj["object_id"]: obj for obj in fixture["objects"]}

    def domain(obj: dict[str, Any]) -> tuple[str, str, str]:
        return obj["connector"], obj["service_id"], obj["physical_subject_sha256"]

    source_domains = {domain(objects[entry["bindings"]["source"]]) for entry in fixture["fixtures"]}
    claims = tuple(
        sorted(
            (
                CompositionPhysicalClaim(
                    cast(Literal["mssql", "clickhouse", "postgres"], part.connector),
                    part.service_id,
                    part.physical_subject_sha256,
                    label_sha("opaque test observation"),
                    "mutation_and_retained_source"
                    if (part.connector, part.service_id, part.physical_subject_sha256) in source_domains
                    else "mutation",
                    part.read_relations,
                    part.write_relations,
                )
                for part in grant.scope.participants
            ),
            key=lambda claim: claim.guard_id,
        )
    )
    owner = CompositionQualificationOwner(
        grant.qualification_run_id,
        grant.consumption_subject_sha256,
        grant.grant_sha256,
        grant.fixture_plan_sha256,
        grant.qualification_plan_sha256,
        grant.scope.scope_sha256,
        grant.scope.environment_id,
        grant.scope.campaign_id,
        claims,
    )
    domains = {domain(objects[edge["object_id"]]) for edge in row["effects"]}
    operation = CompositionQualificationOperation(
        owner.owner_key,
        owner.subject_sha256,
        owner.qualification_run_id,
        owner.grant_sha256,
        owner.fixture_plan_sha256,
        owner.qualification_plan_sha256,
        row["work_item_id"],
        sha(canonical_document({"schema": "dpone.nonproduction-qualification-work-item.v1", **row})),
        row["action"],
        "10000000-0000-4000-8000-000000000020",
        1,
        tuple(
            (claim.guard_id, 7)
            for claim in claims
            if (claim.connector, claim.service_id, claim.physical_subject_sha256) in domains
        ),
    )
    return owner, operation
