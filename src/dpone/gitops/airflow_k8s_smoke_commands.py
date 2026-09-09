from __future__ import annotations

import json
import shlex
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AirflowK8sSmokeCommandSpec:
    name: str
    kind: str
    command: str
    required: bool
    timeout_seconds: int


def build_airflow_k8s_smoke_commands(
    *,
    kubectl: str,
    airflow_cmd: str,
    smoke_name: str,
    namespace: str,
    service_account: str,
    image_ref: str,
    run_spec_path: str,
    evidence_output: str,
    xcom_summary_path: str,
    runner_kind: str,
    timeout_seconds: int,
) -> tuple[AirflowK8sSmokeCommandSpec, ...]:
    """Build portable kubectl/Airflow smoke commands."""

    kubectl_base = f"{kubectl} --namespace {shlex.quote(namespace)}"
    return (
        AirflowK8sSmokeCommandSpec(
            name="kubectl_auth_can_i",
            kind="kubernetes_rbac",
            command=(
                f"{kubectl_base} auth can-i create pods "
                f"--as system:serviceaccount:{shlex.quote(namespace)}:{shlex.quote(service_account)}"
            ),
            required=True,
            timeout_seconds=timeout_seconds,
        ),
        AirflowK8sSmokeCommandSpec(
            name="kubectl_image_version_smoke",
            kind="kubernetes_image",
            command=_kubectl_run_command(
                kubectl_base=kubectl_base,
                name=f"{smoke_name}-version",
                image_ref=image_ref,
                service_account=service_account,
                command=("dpone", "--version"),
            ),
            required=True,
            timeout_seconds=timeout_seconds,
        ),
        AirflowK8sSmokeCommandSpec(
            name="kubectl_run_spec_smoke",
            kind=runner_kind,
            command=_kubectl_run_command(
                kubectl_base=kubectl_base,
                name=f"{smoke_name}-run-spec",
                image_ref=image_ref,
                service_account=service_account,
                command=(
                    "dpone",
                    "gitops",
                    "airflow",
                    "run-spec-exec",
                    run_spec_path,
                    "--evidence-output",
                    evidence_output,
                    "--xcom-output",
                    xcom_summary_path,
                ),
            ),
            required=True,
            timeout_seconds=timeout_seconds,
        ),
        AirflowK8sSmokeCommandSpec(
            name="airflow_outcome_gate",
            kind="airflow_outcome",
            command=(
                f"{airflow_cmd} version >/dev/null && "
                f"dpone gitops airflow outcome-gate {shlex.quote(xcom_summary_path)} --required-status passed"
            ),
            required=True,
            timeout_seconds=timeout_seconds,
        ),
    )


def _kubectl_run_command(
    *,
    kubectl_base: str,
    name: str,
    image_ref: str,
    service_account: str,
    command: tuple[str, ...],
) -> str:
    overrides = json.dumps(
        {
            "apiVersion": "v1",
            "spec": {"serviceAccountName": service_account},
        },
        separators=(",", ":"),
    )
    command_tail = " ".join(shlex.quote(token) for token in command)
    return (
        f"{kubectl_base} run {shlex.quote(name)} --restart=Never --rm -i "
        f"--overrides={shlex.quote(overrides)} --image {shlex.quote(image_ref)} --command -- {command_tail}"
    )


__all__ = ["AirflowK8sSmokeCommandSpec", "build_airflow_k8s_smoke_commands"]
