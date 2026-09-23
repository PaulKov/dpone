"""Real pure-provider and runtime codec closure tests, with synthetic metadata."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from dpone_airflow_pack.credential_projection_contract import (
    CredentialProjectionError,
    canonical_projection_bytes,
    projection_descriptor,
)
from dpone_airflow_pack.credential_projection_pod import load_credential_projection_context
from dpone_airflow_pack.init_fetch_contract import init_fetch_context_from_payload
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs

from dpone.readiness.airflow_credential_projection import build_credential_projection
from dpone.runtime.runtime_credential_projection import verify_runtime_credential_projection
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from tests.test_airflow_credential_projection import projection_case
from tests.test_airflow_provider_init_fetch_execution import _v2_payload
from tests.test_native_deployment_credential_projection_gap import _native_case


def delivery_case(*, shared=False):
    pack, kwargs, previous = _native_case()
    inputs = projection_case(shared=shared)
    inputs.update(release_id=previous.release_id, requirements={pack["workload"]["workload_id"]: ("warehouse",)})
    projection = build_credential_projection(**inputs)
    body = canonical_projection_bytes(projection.to_dict())
    index = _v2_payload()
    index.update(schema="dpone.airflow-deployment-index.v6", credential_projection=projection_descriptor(body))
    index["workload_packs"] = [{**previous.workload_packs[0].to_dict()}]
    for name in inputs["snapshots"]:
        index[name] = getattr(previous, name).to_dict()
    context = init_fetch_context_from_payload(index)
    return pack, kwargs, replace(context, credential_projection_data=projection), body, inputs, index


@pytest.mark.parametrize("shared", [False, True])
def test_native_v6_builds_exact_base_only_secret_items(shared):
    pack, kwargs, context, _body, _inputs, _index = delivery_case(shared=shared)
    result = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=kwargs,
        context=context,
        workload_id=pack["workload"]["workload_id"],
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    spec = result["full_pod_spec"]["spec"]
    credential_volumes = [volume for volume in spec["volumes"] if volume["name"].startswith("dpone-credentials-")]
    assert len(credential_volumes) == 1
    items = credential_volumes[0]["secret"]["items"]
    assert {item["path"] for item in items} == {"warehouse_runtime/uri", "control_runtime/uri"}
    assert {item["key"] for item in items} == (
        {"AIRFLOW_CONN_SQL_TARGET_LOGIN"}
        if shared
        else {"AIRFLOW_CONN_SQL_TARGET_LOGIN", "AIRFLOW_CONN_SQL_CONTROL_LOGIN"}
    )
    assert all(
        not any(mount["name"].startswith("dpone-credentials-") for mount in container.get("volumeMounts", []))
        for container in spec["initContainers"]
    )
    assert not any(key.startswith("AIRFLOW_CONN_") for key in result["env_vars"])


def test_v6_plan_codec_and_runtime_transitive_descriptor():
    pack, _kwargs, context, body, inputs, _index = delivery_case()
    encoded = context.encode_plan(
        workload_id=pack["workload"]["workload_id"],
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    plan, _sha = decode_runtime_init_fetch_plan(encoded.base64, encoded.sha256)
    assert plan.to_dict()["schema"] == "dpone.airflow-runtime-init-fetch-plan.v6"
    assert plan.credential_projection.to_dict() == projection_descriptor(body)
    deployment = {
        "environment": plan.environment,
        "release_ref": plan.release_id,
        "credential_projection": projection_descriptor(body),
        "binding_set": plan.binding_set.to_dict(),
        "connection_registry": plan.connection_registry.to_dict(),
    }
    payloads = {
        plan.deployment.artifact_ref: json.dumps(deployment).encode(),
        plan.credential_projection.artifact_ref: body,
        plan.binding_set.artifact_ref: inputs["snapshots"]["binding_set"],
        plan.connection_registry.artifact_ref: inputs["snapshots"]["connection_registry"],
    }
    verified = verify_runtime_credential_projection(plan, payloads)
    verified.require_authority(
        control_ref="workspace_control", authority_sha256=inputs["authority"].publish_authority_sha256
    )
    payloads[plan.credential_projection.artifact_ref] = body + b" "
    with pytest.raises(CredentialProjectionError):
        verify_runtime_credential_projection(plan, payloads)


def test_provider_reads_exact_local_projection_and_snapshots(tmp_path):
    _pack, _kwargs, context, body, inputs, _index = delivery_case()
    root = tmp_path / "cache"
    directory = root / "deployments" / "prod" / context.deployment_id.replace(":", "-")
    directory.mkdir(parents=True)
    (directory / "credential-projection.json").write_bytes(body)
    (directory / "binding-set.json").write_bytes(inputs["snapshots"]["binding_set"])
    (directory / "connection-registry.ref").write_bytes(inputs["snapshots"]["connection_registry"])
    loaded = load_credential_projection_context(
        replace(context, credential_projection_data=None),
        index_path=directory / "airflow-index.json",
        cache_root=root,
        activation=None,
    )
    assert loaded.credential_projection_data == context.credential_projection_data


def test_v6_has_no_development_only_requirement():
    _pack, _kwargs, _context, _body, _inputs, index = delivery_case()
    assert init_fetch_context_from_payload(index).development_authority_required is False
    index = deepcopy(index)
    index["trust_tier"] = index["runtime_artifact_delivery"]["trust_tier"] = "non_production"
    index["runtime_artifact_delivery"]["verify"]["attestations"] = "optional"
    index["runtime_artifact_delivery"].pop("trust_policy_ref")
    assert init_fetch_context_from_payload(index).development_authority_required is False


def write_native_environment(tmp_path):
    """Build explicit synthetic target/control registry and protected authority."""
    import yaml

    from tests.test_dbt_airflow_release_e2e import _write_environment

    _write_environment(tmp_path)
    binding_path = tmp_path / "environments/prod/binding-set.yaml"
    bindings = yaml.safe_load(binding_path.read_text())
    bindings["bindings"]["workspace_control"] = {"connection_ref": "control_runtime"}
    binding_path.write_text(yaml.safe_dump(bindings))
    registry_path = tmp_path / "platform/connection-registries/prod.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["connections"]["control_runtime"] = projection_case()["source_registry"]["connections"]["control_runtime"]
    registry_path.write_text(yaml.safe_dump(registry))
    return replace(projection_case()["authority"], artifact_registry_ref="synthetic-artifacts")


def build_native_deployment(tmp_path, *, dbt_image=None):
    """Compile a real synthetic two-project native release and exact v6 deployment."""
    from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
    from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
    from tests.dbt_compact_wire_v2_helpers import IMAGE, SIDECAR, prepare_projects, workspace_service
    from tests.test_dbt_airflow_release_e2e import _config_map_ref

    source, compiled, cache = tmp_path / "workspace", tmp_path / "compiled", tmp_path / ".dpone-cache"
    prepare_projects(source)
    report = workspace_service(tmp_path / "profiles").compile(source, output_dir=compiled)
    assert report.passed
    materialized = materialize_compact_pack_release(pack_root=compiled, cache_root=cache, xcom_sidecar_image=SIDECAR)
    assert materialized.passed, materialized.blockers
    authority = write_native_environment(tmp_path)
    deployment = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=materialized.release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
        desired_state_authority=authority,
        runtime_image_dbt_ref=dbt_image,
        runtime_image_dbt_digest=dbt_image.split("@")[-1] if dbt_image else None,
    )
    return deployment, cache, authority


def test_complete_native_build_validates_sealed_deployment_and_inventory(tmp_path):
    from dpone_airflow_pack.deployment_index_contract import load_airflow_deployment_index

    from dpone.contracts.airflow_deployment_projection import deployment_projection_violation
    from dpone.runtime.airflow_credential_projection_inventory import (
        credential_projection_publication_file,
        verify_credential_projection_files,
    )
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

    projection, cache, authority = build_native_deployment(tmp_path)
    assert projection.deployment["schema"] == "dpone.deployment-set.v6"
    assert deployment_projection_violation(projection.deployment, projection.airflow_index) is None
    DeploymentCacheProjectionValidator(cache).validate_details(projection.deployment_dir, environment="prod")
    verified = verify_credential_projection_files(
        projection.deployment, deployment_dir=projection.deployment_dir, root=cache
    )
    verified.require_authority(control_ref="workspace_control", authority_sha256=authority.publish_authority_sha256)
    indexed = load_airflow_deployment_index(projection.deployment_dir / "airflow-index.json", cache_root=cache)
    assert indexed.delivery_context.credential_projection_data == verified
    published = credential_projection_publication_file(
        projection.deployment, deployment_dir=projection.deployment_dir, root=cache
    )
    assert "cache://" + published.key.as_posix() == projection.deployment["credential_projection"]["artifact_ref"]


def test_native_dbt_image_flavor_matches_v6_schema_and_provider(tmp_path):
    from dpone_airflow_pack.deployment_index_contract import load_airflow_deployment_index

    from dpone.gitops.schema_validation import GitOpsSchemaValidator

    dbt_image = "example.invalid/runtime-dbt@sha256:" + "e" * 64
    projection, cache, _authority = build_native_deployment(tmp_path, dbt_image=dbt_image)
    validator = GitOpsSchemaValidator()
    for payload in (projection.deployment, projection.airflow_index):
        assert validator.validate(payload, expected_kind=payload["schema"]) == ()
        for field in ("runtime_image_dbt_ref", "runtime_image_dbt_digest"):
            missing = deepcopy(payload)
            missing.pop(field)
            assert validator.validate(missing, expected_kind=missing["schema"])
    context = load_airflow_deployment_index(
        projection.deployment_dir / "airflow-index.json", cache_root=cache
    ).delivery_context
    workload_id = next(row.id for row in context.workload_packs if row.id.startswith("dbt__"))
    encoded = context.encode_plan(
        workload_id=workload_id, execution_kind="runtime", execution_scope="workload", hook_execution="externalized"
    )
    plan = json.loads(encoded.payload)
    assert plan["runtime_image"] == {"ref": dbt_image, "digest": dbt_image.split("@")[-1]}
    assert validator.validate(plan, expected_kind=plan["schema"]) == ()


@pytest.mark.parametrize("damage", ["missing", "oversized", "symlink"])
def test_projection_file_damage_has_closed_cache_error(tmp_path, damage):
    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

    projection, cache, _authority = build_native_deployment(tmp_path)
    path = projection.deployment_dir / "credential-projection.json"
    if damage == "oversized":
        path.write_bytes(path.read_bytes() + b"SENSITIVE_SENTINEL")
    else:
        path.unlink()
        if damage == "symlink":
            path.symlink_to(projection.deployment_dir / "binding-set.json")
    with pytest.raises(DeploymentCacheError) as failure:
        DeploymentCacheProjectionValidator(cache).validate_details(projection.deployment_dir, environment="prod")
    assert failure.value.code.startswith("DPONE_RUNTIME_CREDENTIAL_PROJECTION_")
    assert "SENSITIVE_SENTINEL" not in str(failure.value)
