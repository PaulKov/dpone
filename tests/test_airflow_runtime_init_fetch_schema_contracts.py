from __future__ import annotations

from copy import deepcopy

from dpone.gitops.schema_validation import GitOpsSchemaValidator

SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64
SHA_C = "sha256:" + "c" * 64
SHA_D = "sha256:" + "d" * 64
SHA_E = "sha256:" + "e" * 64
RELEASE_DIR = SHA_A.replace(":", "-")
DEPLOYMENT_DIR = SHA_B.replace(":", "-")


def test_runtime_init_fetch_plan_schema_accepts_the_complete_wire() -> None:
    assert _issues(_plan()) == ()


def test_runtime_init_fetch_plan_v2_accepts_external_dbt_payloads() -> None:
    plan = _plan()
    plan["schema"] = "dpone.airflow-runtime-init-fetch-plan.v2"
    plan["runtime_payloads"] = [
        {
            "id": "daily_marts_project",
            "kind": "dbt_project_bundle",
            "artifact_ref": (f"cache://releases/{RELEASE_DIR}/runtime/dbt/daily_marts.project.tar.gz"),
            "sha256": SHA_E,
            "bytes": 4096,
            "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
        }
    ]

    assert _issues(plan) == ()


def test_runtime_init_fetch_plan_v3_accepts_explicit_hook_execution() -> None:
    plan = _plan()
    plan["schema"] = "dpone.airflow-runtime-init-fetch-plan.v3"
    plan["runtime_payloads"] = []
    plan["execution"] = {
        "kind": "runtime",
        "scope": "process",
        "selector": "load_orders",
        "process_selector": "dbo.orders",
        "hook_name": None,
        "hook_execution": "inline",
    }

    assert _issues(plan) == ()


def test_runtime_init_fetch_plan_v3_requires_explicit_execution_scope() -> None:
    plan = _plan()
    plan["schema"] = "dpone.airflow-runtime-init-fetch-plan.v3"
    plan["runtime_payloads"] = []
    plan["execution"] = {
        "kind": "runtime",
        "selector": "load_orders",
        "process_selector": "dbo.orders",
        "hook_name": None,
        "hook_execution": "inline",
    }

    assert any(issue.path == "execution.scope" for issue in _issues(plan))


def test_runtime_init_fetch_plan_schema_rejects_secret_fields_and_attestation_downgrade() -> None:
    secret = _plan()
    secret["registry"]["token"] = "must-not-pass"
    downgraded = _plan()
    downgraded["verify"]["attestations"] = "optional"

    assert _issues(secret)
    assert _issues(downgraded)


def test_runtime_fetch_ready_schema_accepts_required_attestation_decision() -> None:
    assert _issues(_ready()) == ()


def test_runtime_fetch_ready_schema_rejects_unknown_fields_and_policy_downgrade() -> None:
    unknown = _ready()
    unknown["verification"]["timings"] = {"download_ms": 1}
    downgraded = _ready()
    downgraded["verification"]["attestations"] = "not_required"

    assert _issues(unknown)
    assert _issues(downgraded)


def test_runtime_fetch_ready_schema_rejects_coordinated_production_policy_downgrade() -> None:
    downgraded = _ready()
    downgraded["trust_policy_sha256"] = None
    verification = downgraded["verification"]
    assert isinstance(verification, dict)
    verification["effective_attestation_requirement"] = "optional"
    verification["attestations"] = "not_required"

    assert _issues(downgraded)


def test_runtime_fetch_ready_schema_accepts_optional_non_production_decision() -> None:
    ready = _ready()
    ready["trust_tier"] = "non_production"
    ready["trust_policy_sha256"] = None
    ready["verification"]["effective_attestation_requirement"] = "optional"
    ready["verification"]["attestations"] = "not_required"

    assert _issues(ready) == ()


def test_runtime_fetch_ready_v2_accepts_attested_non_production_decision() -> None:
    ready = _ready()
    ready["schema"] = "dpone.runtime-fetch-ready.v2"
    ready["trust_tier"] = "non_production"
    verification = ready["verification"]
    assert isinstance(verification, dict)
    verification["artifact_attestation"] = {
        "subject_kind": "airflow_deployment",
        "backend": "cosign_public_key",
        "attestation_id": SHA_A,
        "verification_sha256": SHA_B,
        "observed_claims": [
            "release_id",
            "deployment_id",
            "environment",
            "artifact_registry_ref",
            "registry_scope_id",
            "release_set_sha256",
            "deployment_sha256",
            "airflow_index_sha256",
            "runtime_image_digest",
        ],
        "unobserved_claims": [],
    }

    assert _issues(ready) == ()


def test_runtime_wire_schemas_reject_cache_path_traversal() -> None:
    plan = _plan()
    plan_release = plan["release"]
    assert isinstance(plan_release, dict)
    plan_release["artifact_ref"] = "cache://../outside.json"

    ready = _ready()
    artifacts = ready["artifacts"]
    assert isinstance(artifacts, dict)
    ready_release = artifacts["release"]
    assert isinstance(ready_release, dict)
    ready_release["artifact_ref"] = "cache://releases/../outside.json"

    assert _issues(plan)
    assert _issues(ready)


def _issues(payload: dict[str, object]) -> tuple[object, ...]:
    kind = str(payload["schema"])
    return GitOpsSchemaValidator().validate(payload, expected_kind=kind)


def _plan() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-runtime-init-fetch-plan.v1",
        "environment": "prod",
        "trust_tier": "production",
        "release_id": SHA_A,
        "deployment_id": SHA_B,
        "runtime_image": {
            "ref": f"registry.example/dpone/runtime@{SHA_C}",
            "digest": SHA_C,
        },
        "registry": {
            "logical_ref": "dpone-prod-artifacts",
            "configuration": _config_ref("registry", SHA_D),
        },
        "trust_policy": _config_ref("trust", SHA_E),
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "airflow-example",
        },
        "release": _artifact(
            f"cache://releases/{RELEASE_DIR}/release-set.json",
            SHA_A,
            1024,
        ),
        "deployment": _artifact(
            f"cache://deployments/prod/{DEPLOYMENT_DIR}/deployment.json",
            SHA_B,
            2048,
        ),
        "binding_set": _runtime_connection_artifact("binding-set.json", SHA_A, 512),
        "connection_registry": _runtime_connection_artifact("connection-registry.json", SHA_B, 768),
        "credential_runtime": _runtime_connection_artifact("credential-runtime.json", SHA_C, 384),
        "workload_pack": {
            "id": "load_orders",
            **_artifact(
                f"cache://releases/{RELEASE_DIR}/packs/load_orders.airflow-pack.json",
                SHA_D,
                4096,
            ),
            "pack_fingerprint": SHA_E,
        },
        "execution": {
            "kind": "runtime",
            "selector": "load_orders",
            "hook_name": None,
        },
        "verify": {
            "checksums": "required",
            "attestations": "required_for_prod",
        },
    }


def _ready() -> dict[str, object]:
    plan = _plan()
    return {
        "schema": "dpone.runtime-fetch-ready.v1",
        "plan_sha256": SHA_E,
        "release_id": SHA_A,
        "deployment_id": SHA_B,
        "runtime_image_digest": SHA_C,
        "trust_tier": "production",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "registry_config_sha256": SHA_D,
        "trust_policy_sha256": SHA_E,
        "workload_id": "load_orders",
        "pack_fingerprint": SHA_E,
        "artifacts": {
            "release": _ready_artifact(
                deepcopy(plan["release"]),
                "payload/releases/release-set.json",
            ),
            "deployment": _ready_artifact(
                deepcopy(plan["deployment"]),
                "payload/deployments/deployment.json",
            ),
            "workload_pack": _ready_artifact(
                deepcopy(plan["workload_pack"]),
                "payload/packs/load_orders.airflow-pack.json",
            ),
        },
        "verification": {
            "checksums": "passed",
            "effective_attestation_requirement": "required",
            "attestations": "passed",
            "attestation_decision_sha256": SHA_E,
            "runtime_payload_sha256": SHA_D,
        },
    }


def _config_ref(name: str, sha256: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-{name}",
        "key": f"{name}.json",
        "sha256": sha256,
    }


def _artifact(artifact_ref: str, sha256: str, size: int) -> dict[str, object]:
    return {
        "artifact_ref": artifact_ref,
        "sha256": sha256,
        "bytes": size,
    }


def _runtime_connection_artifact(filename: str, sha256: str, size: int) -> dict[str, object]:
    context_id = SHA_E.replace(":", "-")
    return _artifact(
        f"cache://runtime-connection-contexts/{context_id}/{filename}",
        sha256,
        size,
    )


def _ready_artifact(value: object, locator: str) -> dict[str, object]:
    assert isinstance(value, dict)
    artifact = {key: item for key, item in value.items() if key in {"artifact_ref", "sha256", "bytes"}}
    artifact["locator"] = locator
    return artifact
