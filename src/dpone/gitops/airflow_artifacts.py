from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_dag_factory_renderer import render_airflow_dag_factory


class GitOpsAirflowArtifactRenderer:
    """Pure renderer for Airflow/Kubernetes handoff files."""

    def pod_template(
        self,
        *,
        image: str,
        namespace: str,
        service_account: str,
        bundle_path: str,
    ) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "dpone-airflow-worker",
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "dpone",
                    "app.kubernetes.io/component": "airflow-runner",
                },
            },
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": service_account,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 10001,
                    "runAsGroup": 10001,
                    "fsGroup": 10001,
                },
                "containers": [
                    {
                        "name": "base",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["/bin/sh", "-c"],
                        "args": ["/opt/dpone/entrypoint.sh"],
                        "env": [
                            {"name": "DPONE_GITOPS_BUNDLE", "value": bundle_path},
                            {"name": "DPONE_GITOPS_WORKTREE", "value": "."},
                        ],
                        "resources": {
                            "requests": {
                                "cpu": "100m",
                                "memory": "256Mi",
                            },
                            "limits": {
                                "cpu": "1",
                                "memory": "1Gi",
                            },
                        },
                    }
                ],
            },
        }

    def executor_config(self, *, image: str, pod_template_path: str) -> str:
        payload = {
            "pod_template_file": pod_template_path,
            "executor_config": {
                "pod_override": {
                    "spec": {
                        "containers": [
                            {
                                "name": "base",
                                "image": image,
                            }
                        ]
                    }
                }
            },
        }
        return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    def airflow_task(
        self,
        *,
        dag_id: str,
        task_id: str,
        image: str,
        namespace: str,
        service_account: str,
        pod_template_path: str,
        entrypoint_path: str,
    ) -> str:
        return (
            '"""Generated dpone GitOps runner task for Airflow Kubernetes environments."""\n'
            "\n"
            "from __future__ import annotations\n"
            "\n"
            "import pendulum\n"
            "from airflow import DAG\n"
            "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
            "\n"
            f"with DAG(dag_id={dag_id!r}, start_date=pendulum.datetime(2026, 1, 1, tz='UTC'), schedule=None) as dag:\n"
            "    KubernetesPodOperator(\n"
            f"        task_id={task_id!r},\n"
            f"        name={task_id!r},\n"
            f"        namespace={namespace!r},\n"
            f"        image={image!r},\n"
            f"        service_account_name={service_account!r},\n"
            f"        pod_template_file={pod_template_path!r},\n"
            "        cmds=['/bin/sh', '-c'],\n"
            f"        arguments=[{entrypoint_path!r}],\n"
            "        get_logs=True,\n"
            "        is_delete_operator_pod=True,\n"
            "    )\n"
        )

    def dag_factory(
        self,
        *,
        task_id: str,
        image: str,
        namespace: str,
        service_account: str,
        run_spec_path: str,
        runtime_profile_path: str,
        runtime_evidence_path: str,
        xcom_summary_path: str,
        outcome_mode: str = "strict_fail",
        env: tuple[dict[str, str], ...],
        labels: Mapping[str, str],
        annotations: Mapping[str, str],
    ) -> str:
        return render_airflow_dag_factory(
            task_id=task_id,
            image=image,
            namespace=namespace,
            service_account=service_account,
            run_spec_path=run_spec_path,
            runtime_profile_path=runtime_profile_path,
            runtime_evidence_path=runtime_evidence_path,
            xcom_summary_path=xcom_summary_path,
            outcome_mode=outcome_mode,
            env=env,
            labels=labels,
            annotations=annotations,
        )

    def outcome_gate_module(self, *, upstream_task_id: str, required_status: str = "passed") -> str:
        return (
            '"""Generated dpone Airflow outcome gate helper."""\n'
            "\n"
            "from __future__ import annotations\n"
            "\n"
            "from typing import Any\n"
            "\n"
            "OUTCOME_GATE_KIND = 'gitops.airflow_outcome_gate'\n"
            "\n"
            "\n"
            "def evaluate_dpone_airflow_xcom_summary(\n"
            "    summary: dict[str, Any], *, required_status: str = "
            f"{required_status!r}\n"
            ") -> dict[str, Any]:\n"
            "    status = str(summary.get('status') or 'unknown')\n"
            "    blockers = summary.get('blockers') or []\n"
            "    passed = status == required_status and not blockers\n"
            "    return {\n"
            "        'kind': OUTCOME_GATE_KIND,\n"
            "        'status': status,\n"
            "        'required_status': required_status,\n"
            "        'passed': passed,\n"
            "        'failed_step': summary.get('failed_step'),\n"
            "        'runtime_evidence_path': summary.get('runtime_evidence_path') or '',\n"
            "        'runtime_evidence_sha256': summary.get('runtime_evidence_sha256'),\n"
            "        'blockers': blockers,\n"
            "    }\n"
            "\n"
            "\n"
            "def airflow_python_operator_callable(*, ti, upstream_task_id: str = "
            f"{upstream_task_id!r}, required_status: str = {required_status!r}) -> dict[str, Any]:\n"
            "    summary = ti.xcom_pull(task_ids=upstream_task_id)\n"
            "    if not isinstance(summary, dict):\n"
            "        raise RuntimeError('dpone GitOps XCom summary is missing or is not a JSON object')\n"
            "    report = evaluate_dpone_airflow_xcom_summary(summary, required_status=required_status)\n"
            "    if not report['passed']:\n"
            "        raise RuntimeError(f'dpone GitOps outcome blocked: {report!r}')\n"
            "    return report\n"
        )

    def entrypoint(self, *, commands: tuple[str, ...]) -> str:
        lines = [
            "#!/bin/sh",
            "set -eu",
            "",
            'echo "dpone GitOps Airflow runner starting"',
        ]
        lines.extend(commands)
        lines.append('echo "dpone GitOps Airflow runner finished"')
        return "\n".join(lines) + "\n"

    def commands_for_bundle(
        self,
        *,
        bundle_path: str,
        bundle: Mapping[str, Any],
        require_attestation: bool,
    ) -> tuple[str, ...]:
        commands = [
            _bundle_verify_command(bundle_path=bundle_path, require_attestation=require_attestation),
        ]
        verify_lock = bool(_mapping(bundle.get("policy")).get("verify_lock"))
        for entry in _entries(bundle):
            plan_path = str(entry.get("plan_path") or "").strip()
            manifest = str(entry.get("manifest") or "").strip()
            if plan_path:
                commands.append(_verify_command(plan_path=plan_path, verify_lock=verify_lock))
            if manifest:
                commands.append(f"dpone run {shlex.quote(manifest)}")
        return tuple(commands)


def _bundle_verify_command(*, bundle_path: str, require_attestation: bool) -> str:
    command = f"dpone gitops bundle verify {shlex.quote(bundle_path)}"
    if require_attestation:
        command += " --require-attestation"
    return command


def _verify_command(*, plan_path: str, verify_lock: bool) -> str:
    command = f"dpone gitops verify {shlex.quote(plan_path)} --worktree ."
    if verify_lock:
        command += " --verify-lock"
    return command


def _entries(bundle: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_entries = bundle.get("entries")
    if not isinstance(raw_entries, list):
        return ()
    return tuple(entry for entry in raw_entries if isinstance(entry, Mapping))


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["GitOpsAirflowArtifactRenderer"]
