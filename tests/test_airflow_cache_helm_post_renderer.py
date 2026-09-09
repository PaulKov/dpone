from __future__ import annotations

import subprocess
import sys
from copy import deepcopy

import pytest
import yaml
from dpone_airflow_pack.helm_ack_mounts import (
    LoaderAckMountPolicyError,
    harden_loader_ack_mounts,
    verify_loader_ack_mounts,
)


@pytest.mark.parametrize("parser_name", ["scheduler", "dag-processor"])
def test_post_renderer_grants_loader_ack_write_only_to_parser(parser_name: str) -> None:
    manifest = _deployment(parser_name)

    rendered = harden_loader_ack_mounts([manifest])

    pod_spec = rendered[0]["spec"]["template"]["spec"]
    mounts = {
        container["name"]: [
            mount for mount in container.get("volumeMounts", []) if mount["name"] == "dpone-airflow-loader-ack"
        ]
        for container in (*pod_spec["initContainers"], *pod_spec["containers"])
    }
    assert mounts[parser_name] == [
        {
            "name": "dpone-airflow-loader-ack",
            "mountPath": "/opt/airflow/.dpone-ack",
            "readOnly": False,
        }
    ]
    assert mounts["dpone-cache-status-projector"][0]["readOnly"] is True
    assert mounts["dpone-cache-watch"][0]["readOnly"] is True
    assert mounts["wait-for-airflow-migrations"] == []
    assert mounts[f"{parser_name}-log-groomer"] == []
    verify_loader_ack_mounts(rendered)


def test_post_renderer_fails_closed_without_exact_parser_container() -> None:
    manifest = _deployment("scheduler")
    manifest["spec"]["template"]["spec"]["containers"][0]["name"] = "renamed-scheduler"

    with pytest.raises(LoaderAckMountPolicyError, match="exactly one parser"):
        harden_loader_ack_mounts([manifest])


def test_verifier_rejects_auxiliary_loader_ack_writer() -> None:
    manifest = _deployment("scheduler")

    with pytest.raises(LoaderAckMountPolicyError, match="wait-for-airflow-migrations"):
        verify_loader_ack_mounts([manifest])


def test_verifier_rejects_ephemeral_debug_container_loader_ack_writer() -> None:
    manifest = harden_loader_ack_mounts([_deployment("scheduler")])[0]
    pod_spec = manifest["spec"]["template"]["spec"]
    pod_spec["ephemeralContainers"] = [
        {
            "name": "debug-shell",
            "volumeMounts": [
                {
                    "name": "dpone-airflow-loader-ack",
                    "mountPath": "/opt/airflow/.dpone-ack",
                    "readOnly": False,
                }
            ],
        }
    ]

    with pytest.raises(LoaderAckMountPolicyError, match="debug-shell"):
        verify_loader_ack_mounts([manifest])


def test_post_renderer_removes_loader_ack_from_ephemeral_debug_container() -> None:
    manifest = _deployment("scheduler")
    pod_spec = manifest["spec"]["template"]["spec"]
    pod_spec["ephemeralContainers"] = [
        {
            "name": "debug-shell",
            "volumeMounts": [
                {
                    "name": "dpone-airflow-loader-ack",
                    "mountPath": "/opt/airflow/.dpone-ack",
                    "readOnly": False,
                }
            ],
        }
    ]

    rendered = harden_loader_ack_mounts([manifest])

    debug = rendered[0]["spec"]["template"]["spec"]["ephemeralContainers"][0]
    assert "volumeMounts" not in debug
    verify_loader_ack_mounts(rendered)


def test_post_renderer_rejects_two_ack_bearing_parser_workloads() -> None:
    with pytest.raises(LoaderAckMountPolicyError, match="exactly one ACK-bearing"):
        harden_loader_ack_mounts([_deployment("scheduler"), _deployment("dag-processor")])


def test_post_renderer_does_not_mutate_input_documents() -> None:
    manifest = _deployment("scheduler")
    original = deepcopy(manifest)

    harden_loader_ack_mounts([manifest])

    assert manifest == original


@pytest.mark.parametrize(
    ("kind", "spec_path"),
    [
        ("Pod", ("spec",)),
        ("Job", ("spec", "template", "spec")),
        ("CronJob", ("spec", "jobTemplate", "spec", "template", "spec")),
        ("DaemonSet", ("spec", "template", "spec")),
        ("PodTemplate", ("spec", "template", "spec")),
        ("ReplicaSet", ("spec", "template", "spec")),
        ("ReplicationController", ("spec", "template", "spec")),
        ("StatefulSet", ("spec", "template", "spec")),
    ],
)
def test_post_renderer_hardens_standard_ack_bearing_workloads(
    kind: str,
    spec_path: tuple[str, ...],
) -> None:
    manifest = _workload(kind, "scheduler")

    rendered = harden_loader_ack_mounts([manifest])

    pod_spec = rendered[0]
    for key in spec_path:
        pod_spec = pod_spec[key]
    parser_mount = next(item for item in pod_spec["containers"] if item["name"] == "scheduler")["volumeMounts"]
    assert parser_mount == [
        {
            "name": "dpone-airflow-loader-ack",
            "mountPath": "/opt/airflow/.dpone-ack",
            "readOnly": False,
        }
    ]
    verify_loader_ack_mounts(rendered)


def test_post_renderer_hardens_ack_workload_inside_kubernetes_list() -> None:
    manifest = {
        "apiVersion": "v1",
        "kind": "List",
        "items": [_workload("StatefulSet", "scheduler")],
    }

    rendered = harden_loader_ack_mounts([manifest])

    pod_spec = rendered[0]["items"][0]["spec"]["template"]["spec"]
    parser_mount = next(item for item in pod_spec["containers"] if item["name"] == "scheduler")["volumeMounts"]
    assert parser_mount[0]["readOnly"] is False
    verify_loader_ack_mounts(rendered)


def test_post_renderer_fails_closed_for_unknown_ack_bearing_resource() -> None:
    manifest = {
        "apiVersion": "example.test/v1",
        "kind": "CustomWorkload",
        "metadata": {"name": "unsafe"},
        "spec": _deployment("scheduler")["spec"],
    }

    with pytest.raises(LoaderAckMountPolicyError, match="unsupported ACK-bearing"):
        harden_loader_ack_mounts([manifest])


@pytest.mark.parametrize("raw", [b"", b"kind: [unterminated"])
def test_post_renderer_cli_rejects_empty_or_malformed_input(raw: bytes) -> None:
    result = _run_post_renderer(raw)

    assert result.returncode == 2
    assert b"DPONE_AIRFLOW_LOADER_ACK_MOUNT_INVALID" in result.stderr


def test_post_renderer_cli_enforces_exact_input_size_boundary() -> None:
    limit = 32 * 1024 * 1024
    hardened = harden_loader_ack_mounts([_deployment("scheduler")])
    rendered = yaml.safe_dump_all(hardened, explicit_start=True, sort_keys=False).encode("utf-8")
    exact_limit = rendered + b"\n#" + (b"x" * (limit - len(rendered) - 2))

    accepted = _run_post_renderer(exact_limit, "--verify-only")
    rejected = _run_post_renderer(exact_limit + b"x", "--verify-only")

    assert len(exact_limit) == limit
    assert accepted.returncode == 0
    assert accepted.stderr == b""
    assert rejected.returncode == 2
    assert b"32 MiB safety limit" in rejected.stderr


def _run_post_renderer(raw: bytes, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-m", "dpone_airflow_pack.cli_helm_post_renderer", *args],
        input=raw,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
    )


def _deployment(parser_name: str) -> dict[str, object]:
    unsafe_mount = {
        "name": "dpone-airflow-loader-ack",
        "mountPath": "/opt/airflow/.dpone-ack",
        "readOnly": False,
    }
    readonly_mount = {**unsafe_mount, "readOnly": True}
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": f"airflow-{parser_name}"},
        "spec": {
            "template": {
                "spec": {
                    "volumes": [{"name": "dpone-airflow-loader-ack", "emptyDir": {}}],
                    "initContainers": [
                        {
                            "name": "wait-for-airflow-migrations",
                            "volumeMounts": [dict(unsafe_mount)],
                        }
                    ],
                    "containers": [
                        {"name": parser_name, "volumeMounts": [dict(unsafe_mount)]},
                        {
                            "name": "dpone-cache-status-projector",
                            "volumeMounts": [dict(readonly_mount)],
                        },
                        {
                            "name": "dpone-cache-watch",
                            "volumeMounts": [dict(readonly_mount)],
                        },
                        {
                            "name": f"{parser_name}-log-groomer",
                            "volumeMounts": [dict(unsafe_mount)],
                        },
                    ],
                }
            }
        },
    }


def _workload(kind: str, parser_name: str) -> dict[str, object]:
    pod_spec = _deployment(parser_name)["spec"]["template"]["spec"]
    if kind == "Pod":
        return {"apiVersion": "v1", "kind": kind, "metadata": {"name": "parser"}, "spec": pod_spec}
    template = {"metadata": {"labels": {"app": "parser"}}, "spec": pod_spec}
    if kind == "CronJob":
        spec = {"jobTemplate": {"spec": {"template": template}}}
    else:
        spec = {"template": template}
    return {"apiVersion": "batch/v1", "kind": kind, "metadata": {"name": "parser"}, "spec": spec}
