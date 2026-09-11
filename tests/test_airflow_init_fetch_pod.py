"""Public provider contract for protected v3 composition runtime Pods."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from copy import deepcopy
from typing import Any

import pytest
from dpone_airflow_pack.init_fetch_contract import (
    InitFetchProviderError,
    init_fetch_context_from_payload,
)
from dpone_airflow_pack.init_fetch_pod import (
    apply_composition_supervisor,
    compose_init_fetch_operator_kwargs,
)
from dpone_airflow_pack.run_identity import COMPOSITION_SUPERVISOR_B64_ENV

from tests.test_airflow_provider_init_fetch_execution import (
    _strict_pack,
    _v2_payload,
)

SUPERVISOR = {
    "schema": "dpone.composition-supervisor.v1",
    "persistent_volume_claim": "dpone-composition-supervisor",
    "child_uid_start": 1_000_000_000,
    "child_gid_start": 1_100_000_000,
    "child_identity_count": 1_000_000,
}
SUPERVISOR_SECURITY_CONTEXT = {
    "runAsUser": 0,
    "runAsGroup": 0,
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "seccompProfile": {"type": "RuntimeDefault"},
    "capabilities": {
        "drop": ["ALL"],
        "add": [
            "CHOWN",
            "FOWNER",
            "DAC_READ_SEARCH",
            "SETUID",
            "SETGID",
            "KILL",
        ],
    },
}


def _v3_payload() -> dict[str, Any]:
    payload = _v2_payload()
    payload["schema"] = "dpone.airflow-deployment-index.v3"
    payload["composition_supervisor"] = deepcopy(SUPERVISOR)
    return payload


def _compose(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    pack = _strict_pack()
    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=init_fetch_context_from_payload(payload or _v3_payload()),
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    return kwargs


def _named(items: list[dict[str, Any]], name: str) -> dict[str, Any]:
    return next(item for item in items if item["name"] == name)


def test_v3_runtime_pod_has_exact_supervisor_boundary() -> None:
    kwargs = _compose()
    pod = kwargs["full_pod_spec"]
    base = pod["spec"]["containers"][0]
    init = pod["spec"]["initContainers"][0]

    assert base["securityContext"] == SUPERVISOR_SECURITY_CONTEXT
    assert _named(pod["spec"]["volumes"], "dpone-composition-supervisor") == {
        "name": "dpone-composition-supervisor",
        "persistentVolumeClaim": {
            "claimName": "dpone-composition-supervisor",
        },
    }
    assert _named(pod["spec"]["volumes"], "dpone-composition-profiles") == {
        "name": "dpone-composition-profiles",
        "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"},
    }
    assert _named(base["volumeMounts"], "dpone-composition-supervisor") == {
        "name": "dpone-composition-supervisor",
        "mountPath": "/var/lib/dpone/composition",
        "readOnly": False,
    }
    assert _named(base["volumeMounts"], "dpone-composition-profiles") == {
        "name": "dpone-composition-profiles",
        "mountPath": "/dev/shm/dpone-composition",
        "readOnly": False,
    }
    assert _named(base["volumeMounts"], "dpone-worktree")["readOnly"] is True
    assert all(
        mount["name"] not in {"dpone-composition-supervisor", "dpone-composition-profiles"}
        for mount in init["volumeMounts"]
    )

    encoded = kwargs["env_vars"][COMPOSITION_SUPERVISOR_B64_ENV]
    assert {item["name"]: item["value"] for item in base["env"]}[COMPOSITION_SUPERVISOR_B64_ENV] == encoded
    assert COMPOSITION_SUPERVISOR_B64_ENV not in {item["name"] for item in init["env"]}
    canonical = base64.b64decode(encoded, validate=True)
    assert canonical == json.dumps(
        SUPERVISOR,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    assert set(json.loads(canonical)) == set(SUPERVISOR)
    assert not any(marker in canonical.lower() for marker in (b"password", b"credential", b"secret", b"token"))


def test_v2_runtime_pod_retains_ordinary_provider_contract() -> None:
    kwargs = _compose(_v2_payload())
    pod = kwargs["full_pod_spec"]
    base = pod["spec"]["containers"][0]

    assert COMPOSITION_SUPERVISOR_B64_ENV not in kwargs["env_vars"]
    assert "securityContext" not in base
    assert {item["name"] for item in pod["spec"]["volumes"]}.isdisjoint(
        {"dpone-composition-supervisor", "dpone-composition-profiles"}
    )
    assert {item["name"] for item in base["volumeMounts"]}.isdisjoint(
        {"dpone-composition-supervisor", "dpone-composition-profiles"}
    )


def test_public_operator_rejects_pack_supervisor_env_at_provider_env_guard() -> None:
    pack = _strict_pack()
    kwargs = deepcopy(pack["provider_execution"]["kpo_kwargs"])
    kwargs["env_vars"][COMPOSITION_SUPERVISOR_B64_ENV] = "forged"

    with pytest.raises(InitFetchProviderError) as raised:
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=init_fetch_context_from_payload(_v3_payload()),
            workload_id="orders",
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )

    assert raised.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"
    assert str(raised.value) == (
        "strict init-fetch rejects non-contract environment variables: DPONE_COMPOSITION_SUPERVISOR_B64"
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda pod: pod["spec"].update(securityContext={"runAsUser": 1_000}),
            "supervisor pod cannot preconfigure pod security fields: securityContext",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].update(securityContext={"privileged": True}),
            "supervisor pod cannot preconfigure container security",
        ),
        (
            lambda pod: pod["spec"]["volumes"].append({"name": "dpone-composition-supervisor", "emptyDir": {}}),
            "supervisor pod contains a reserved volume collision",
        ),
        (
            lambda pod: pod["spec"]["containers"][0]["volumeMounts"].append(
                {
                    "name": "attacker",
                    "mountPath": "/dev/shm/dpone-composition",
                }
            ),
            "supervisor pod contains a reserved mount collision",
        ),
        (
            lambda pod: pod["spec"]["containers"][0]["env"].append(
                {"name": COMPOSITION_SUPERVISOR_B64_ENV, "value": "forged"}
            ),
            "supervisor pod contains a reserved environment collision",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].pop("env"),
            "supervisor pod container env must be a list",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].update(env={}),
            "supervisor pod container env must be a list",
        ),
        (
            lambda pod: pod["spec"]["initContainers"][0].pop("env"),
            "supervisor pod container env must be a list",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].pop("volumeMounts"),
            "supervisor pod container volumeMounts must be a list",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].update(volumeMounts={}),
            "supervisor pod container volumeMounts must be a list",
        ),
        (
            lambda pod: pod["spec"]["initContainers"][0].pop("volumeMounts"),
            "supervisor pod container volumeMounts must be a list",
        ),
        (
            lambda pod: pod["spec"].pop("volumes"),
            "supervisor pod volumes must be a list",
        ),
        (
            lambda pod: pod["spec"].update(volumes={}),
            "supervisor pod volumes must be a list",
        ),
    ],
)
def test_exported_supervisor_policy_is_atomic_defense_in_depth(
    mutate: Callable[[dict[str, Any]], object],
    message: str,
) -> None:
    """Direct policy coverage protects future provider-owned Pod composers."""

    pod = _compose(_v2_payload())["full_pod_spec"]
    projection = init_fetch_context_from_payload(_v3_payload()).composition_supervisor
    assert projection is not None
    mutate(pod)
    malformed_pod = deepcopy(pod)

    with pytest.raises(InitFetchProviderError) as raised:
        apply_composition_supervisor(pod, projection)

    assert raised.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"
    assert str(raised.value) == message
    assert pod == malformed_pod


@pytest.mark.parametrize(
    "mutate",
    [
        lambda projection: projection.pop("persistent_volume_claim"),
        lambda projection: projection.update(persistent_volume_claim=""),
        lambda projection: projection.pop("child_gid_start"),
    ],
)
def test_v3_runtime_pod_rejects_missing_or_invalid_supervisor_projection(
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    payload = _v3_payload()
    mutate(payload["composition_supervisor"])

    with pytest.raises(InitFetchProviderError) as raised:
        init_fetch_context_from_payload(payload)

    assert raised.value.code == "DPONE_COMPOSITION_SUPERVISOR_INVALID"
    assert str(raised.value) == "composition supervisor deployment capability is invalid"
