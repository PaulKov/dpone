"""Deployment profile renderers for self-service production handoff."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DeploymentProfileReport:
    passed: bool
    target: str
    manifest_path: str
    selector: str | None
    artifact_path: str
    json_path: str
    markdown_path: str
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        return "\n".join(
            [
                "# dpone deployment profile",
                "",
                f"- Passed: `{self.passed}`",
                f"- Target: `{self.target}`",
                f"- Manifest: `{self.manifest_path}`",
                f"- Selector: `{self.selector or '-'}`",
                f"- Artifact: `{self.artifact_path}`",
                "",
                "## Runbook",
                "",
                "1. Review generated commands before deploying to a scheduler.",
                "2. Inject credentials through the platform secret manager, not the generated artifact.",
                "3. Keep `dpone orchestrate run` as the scheduled command for locks, resume, and evidence.",
                "4. Attach generated deployment profile to environment promotion evidence.",
                "",
            ]
        )


class DeploymentProfileRenderer:
    """Renders Docker Compose, Kubernetes, Airflow, and Dagster handoff templates."""

    _EXTENSIONS = {
        "docker-compose": "docker-compose.yml",
        "k8s-cronjob": "k8s-cronjob.yml",
        "airflow": "airflow_dag.py",
        "dagster": "dagster_asset.py",
    }

    def render(
        self,
        *,
        output_dir: str | Path,
        target: str,
        manifest_path: str,
        selector: str | None = None,
        image: str = "ghcr.io/paulkov/dpone:latest",
        schedule: str = "0 2 * * *",
    ) -> DeploymentProfileReport:
        if target not in self._EXTENSIONS:
            raise ValueError(f"Unsupported deployment target: {target}")
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        artifact_path = directory / self._EXTENSIONS[target]
        json_path = directory / "deployment_profile.json"
        markdown_path = directory / "deployment_profile.md"
        artifact_path.write_text(
            self._render_target(
                target=target,
                manifest_path=manifest_path,
                selector=selector,
                image=image,
                schedule=schedule,
            ),
            encoding="utf-8",
        )
        report = DeploymentProfileReport(
            passed=True,
            target=target,
            manifest_path=manifest_path,
            selector=selector,
            artifact_path=str(artifact_path),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
            output_dir=str(directory),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _render_target(
        self,
        *,
        target: str,
        manifest_path: str,
        selector: str | None,
        image: str,
        schedule: str,
    ) -> str:
        command = _orchestrate_command(manifest_path, selector)
        if target == "docker-compose":
            return _docker_compose(image, command)
        if target == "k8s-cronjob":
            return _k8s_cronjob(image, command, schedule)
        if target == "airflow":
            return _airflow_dag(command, schedule)
        if target == "dagster":
            return _dagster_asset(command)
        raise ValueError(f"Unsupported deployment target: {target}")


def _orchestrate_command(manifest_path: str, selector: str | None) -> str:
    selector_arg = f" --selector {selector}" if selector else ""
    return f"dpone orchestrate run --manifest {manifest_path}{selector_arg} --resume-policy resume"


def _docker_compose(image: str, command: str) -> str:
    return f"""services:
  dpone:
    image: {image}
    command: ["sh", "-lc", "{command}"]
    env_file:
      - .env
    volumes:
      - ./:/workspace
    working_dir: /workspace
"""


def _k8s_cronjob(image: str, command: str, schedule: str) -> str:
    return f"""apiVersion: batch/v1
kind: CronJob
metadata:
  name: dpone-pipeline
spec:
  schedule: "{schedule}"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: dpone
              image: {image}
              command: ["sh", "-lc", "{command}"]
              envFrom:
                - secretRef:
                    name: dpone-secrets
"""


def _airflow_dag(command: str, schedule: str) -> str:
    return f'''from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="dpone_pipeline",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="{schedule}",
    catchup=False,
) as dag:
    BashOperator(task_id="dpone_run", bash_command="{command}")
'''


def _dagster_asset(command: str) -> str:
    return f"""from __future__ import annotations

import subprocess

from dagster import asset


@asset(name="dpone_pipeline")
def dpone_pipeline() -> None:
    subprocess.run({command!r}.split(), check=True)
"""
