"""Closed, value-free runtime-authority projection into strict KPO pods."""

from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError, init_fetch_context_from_payload
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs
from dpone_airflow_pack.init_fetch_pod_guard import validate_strict_operator_overrides
from dpone_airflow_pack.runtime_authority_projection import (
    RUNTIME_AUTHORITY_DIRECTORY,
    RUNTIME_AUTHORITY_PATH,
    RUNTIME_AUTHORITY_PATH_ENV,
    RUNTIME_AUTHORITY_VOLUME,
    patch_pod_spec_runtime_authority,
)

from tests.test_airflow_provider_execution_authority import _strict_pack
from tests.test_airflow_provider_init_fetch_execution import _v2_payload


def _development_index() -> dict[str, object]:
    payload = _v2_payload(trust_policy=False)
    payload["schema"] = "dpone.airflow-deployment-index.v4"
    payload["development_authority_required"] = True
    delivery = payload["runtime_artifact_delivery"]
    assert isinstance(delivery, dict)
    delivery["runtime_authority"] = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-runtime-authority",
        "secret_key": "authority.json",
    }
    return payload


def _env(container: dict[str, object]) -> dict[str, str]:
    return {str(item["name"]): str(item["value"]) for item in container["env"]}  # type: ignore[index]


def test_development_plan_projects_one_read_only_source_to_init_and_base() -> None:
    context = init_fetch_context_from_payload(_development_index())
    kwargs = compose_init_fetch_operator_kwargs(
        pack=_strict_pack(),
        kwargs=_strict_pack()["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    pod = kwargs["full_pod_spec"]
    volumes = pod["spec"]["volumes"]
    authority_volume = next(item for item in volumes if item["name"] == RUNTIME_AUTHORITY_VOLUME)
    assert authority_volume == {
        "name": RUNTIME_AUTHORITY_VOLUME,
        "secret": {
            "secretName": "dpone-runtime-authority",
            "items": [{"key": "authority.json", "path": "authority"}],
            "optional": False,
        },
    }
    containers = [*pod["spec"]["initContainers"], *pod["spec"]["containers"]]
    assert {container["name"] for container in containers} == {"dpone-runtime-init-fetch", "base"}
    for container in containers:
        assert {
            "name": RUNTIME_AUTHORITY_VOLUME,
            "mountPath": RUNTIME_AUTHORITY_DIRECTORY,
            "readOnly": True,
        } in container["volumeMounts"]
        assert _env(container)[RUNTIME_AUTHORITY_PATH_ENV] == RUNTIME_AUTHORITY_PATH

    serialized = json.dumps(kwargs, sort_keys=True)
    assert "synthetic-secret-value" not in serialized
    assert "password" not in serialized.lower()


def test_object_pod_projection_updates_init_and_base_without_replacing_other_fields() -> None:
    base = SimpleNamespace(name="base", env=[], volume_mounts=[SimpleNamespace(name="existing")])
    init = SimpleNamespace(name="dpone-runtime-init-fetch", env=[], volume_mounts=[])
    pod = SimpleNamespace(
        spec=SimpleNamespace(
            containers=[base],
            init_containers=[init],
            volumes=[SimpleNamespace(name="existing")],
        )
    )
    source = init_fetch_context_from_payload(_development_index()).runtime_authority

    result = patch_pod_spec_runtime_authority(pod, source)

    assert result is pod
    authority_volume = pod.spec.volumes[-1]
    assert authority_volume.name == RUNTIME_AUTHORITY_VOLUME
    assert authority_volume.secret.secret_name == "dpone-runtime-authority"
    assert authority_volume.secret.items[0].key == "authority.json"
    assert authority_volume.secret.items[0].path == "authority"
    assert authority_volume.secret.optional is False
    for container in (base, init):
        authority_mount = container.volume_mounts[-1]
        assert authority_mount.name == RUNTIME_AUTHORITY_VOLUME
        assert authority_mount.mount_path == RUNTIME_AUTHORITY_DIRECTORY
        assert authority_mount.read_only is True
        assert [item.value for item in container.env if item.name == RUNTIME_AUTHORITY_PATH_ENV] == [
            RUNTIME_AUTHORITY_PATH
        ]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "unknown_mode",
        "invalid_name",
        "invalid_key",
        "unknown_field",
    ],
)
def test_development_index_requires_closed_runtime_authority_source(mutation: str) -> None:
    payload = _development_index()
    delivery = payload["runtime_artifact_delivery"]
    assert isinstance(delivery, dict)
    source = delivery["runtime_authority"]
    assert isinstance(source, dict)
    if mutation == "missing":
        delivery.pop("runtime_authority")
    elif mutation == "unknown_mode":
        source["mode"] = "inline"
    elif mutation == "invalid_name":
        source["secret_name"] = "Not_A_DNS_Name"
    elif mutation == "invalid_key":
        source["secret_key"] = "../authority"
    else:
        source["value"] = "synthetic-secret-value"

    with pytest.raises(InitFetchProviderError) as exc_info:
        init_fetch_context_from_payload(payload)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert "synthetic-secret-value" not in str(exc_info.value)


def test_ordinary_and_production_index_reject_runtime_authority_projection() -> None:
    payload = _v2_payload()
    delivery = payload["runtime_artifact_delivery"]
    development_delivery = _development_index()["runtime_artifact_delivery"]
    assert isinstance(delivery, dict)
    assert isinstance(development_delivery, dict)
    delivery["runtime_authority"] = deepcopy(development_delivery["runtime_authority"])

    with pytest.raises(InitFetchProviderError) as exc_info:
        init_fetch_context_from_payload(payload)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"


def test_no_authority_plan_has_no_projection() -> None:
    context = init_fetch_context_from_payload(_v2_payload())
    kwargs = compose_init_fetch_operator_kwargs(
        pack=_strict_pack(),
        kwargs=_strict_pack()["provider_execution"]["kpo_kwargs"],
        context=context,
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    serialized = json.dumps(kwargs, sort_keys=True)

    assert RUNTIME_AUTHORITY_VOLUME not in serialized
    assert RUNTIME_AUTHORITY_PATH_ENV not in serialized


def test_operator_overrides_cannot_replace_runtime_authority_contract() -> None:
    for field in ("runtime_authority", "volumes", "volume_mounts", "env_vars"):
        with pytest.raises(InitFetchProviderError, match="cannot replace strict init-fetch fields"):
            validate_strict_operator_overrides({field: {}})
