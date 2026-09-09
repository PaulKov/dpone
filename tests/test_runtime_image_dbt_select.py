"""Dual-digest runtime image selection for dbt__* workloads."""

from __future__ import annotations

from dpone_airflow_pack.init_fetch_contract import InitFetchDeliveryContext


def _context(**overrides):
    base = {
        "environment": "dev",
        "trust_tier": "non_production",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "runtime_image_ref": "harbor.example/dpone@sha256:" + "c" * 64,
        "runtime_image_digest": "sha256:" + "c" * 64,
        "artifact_registry_ref": "registry://dev",
        "registry_configuration": type(
            "CM",
            (),
            {"name": "reg", "key": "registry.json", "sha256": "sha256:" + "d" * 64, "to_dict": lambda self: {}},
        )(),
        "trust_policy": None,
        "identity": type(
            "Id",
            (),
            {"namespace": "ns", "service_account": "sa", "to_dict": lambda self: {}},
        )(),
        "release": type(
            "A",
            (),
            {"artifact_ref": "cache://r", "sha256": "sha256:" + "e" * 64, "bytes": 1, "to_dict": lambda self: {}},
        )(),
        "deployment": type(
            "A",
            (),
            {"artifact_ref": "cache://d", "sha256": "sha256:" + "f" * 64, "bytes": 1, "to_dict": lambda self: {}},
        )(),
        "binding_set": type(
            "A",
            (),
            {"artifact_ref": "cache://b", "sha256": "sha256:" + "1" * 64, "bytes": 1, "to_dict": lambda self: {}},
        )(),
        "connection_registry": type(
            "A",
            (),
            {"artifact_ref": "cache://c", "sha256": "sha256:" + "2" * 64, "bytes": 1, "to_dict": lambda self: {}},
        )(),
        "credential_runtime": type(
            "A",
            (),
            {"artifact_ref": "cache://k", "sha256": "sha256:" + "3" * 64, "bytes": 1, "to_dict": lambda self: {}},
        )(),
        "workload_packs": (),
        "runtime_payloads": (),
        "verify": type("V", (), {"to_dict": lambda self: {}})(),
        "dev_evidence_delivery": None,
    }
    base.update(overrides)
    return InitFetchDeliveryContext(**base)


def test_selects_shared_image_by_default() -> None:
    ctx = _context()
    ref, digest = ctx.runtime_image_for_workload("mssql_sale_plan_type1_smoke")
    assert digest == ctx.runtime_image_digest
    assert ref == ctx.runtime_image_ref


def test_selects_dbt_image_for_dbt_workload_when_present() -> None:
    dbt_digest = "sha256:" + "9" * 64
    dbt_ref = f"harbor.example/dpone-dbt@{dbt_digest}"
    ctx = _context(runtime_image_dbt_ref=dbt_ref, runtime_image_dbt_digest=dbt_digest)
    ref, digest = ctx.runtime_image_for_workload("dbt__example_customer_mart_test")
    assert ref == dbt_ref
    assert digest == dbt_digest


def test_dbt_workload_falls_back_without_dbt_digest() -> None:
    ctx = _context()
    ref, digest = ctx.runtime_image_for_workload("dbt__example_customer_mart_test")
    assert digest == ctx.runtime_image_digest
    assert ref == ctx.runtime_image_ref
