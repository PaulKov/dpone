from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_artifacts import GitOpsAirflowArtifactRenderer
from dpone.gitops.airflow_run_spec import GitOpsAirflowRunSpecBuilder
from dpone.gitops.airflow_runtime_profile import (
    GitOpsAirflowRuntimeProfileBuilder,
    GitOpsAirflowRuntimeProfileInput,
)
from dpone.services.gitops.airflow_image_contract_service import build_image_contract


class GitOpsAirflowArtifactWriter:
    """Writes the deterministic artifact set produced by `dpone gitops airflow render`."""

    def __init__(self, *, ctx: Any) -> None:
        self._ctx = ctx
        self._renderer = GitOpsAirflowArtifactRenderer()
        self._run_spec_builder = GitOpsAirflowRunSpecBuilder()
        self._runtime_profile_builder = GitOpsAirflowRuntimeProfileBuilder()

    def write(
        self,
        *,
        args: object,
        repo_root: Path,
        bundle_path: str,
        bundle: Mapping[str, Any],
        output_dir: Path,
        image_contract_path: Path,
        run_spec_path: Path,
        evidence_path: Path,
        runtime_profile_path: Path,
        xcom_summary_path: Path,
        dag_factory_path: Path,
        outcome_gate_path: Path,
        image: str,
        commands: tuple[str, ...],
    ) -> None:
        pod_template_path = output_dir / "pod_template.yaml"
        entrypoint_path = output_dir / "entrypoint.sh"
        namespace = str(getattr(args, "namespace", "default") or "default")
        service_account = str(getattr(args, "service_account", "default") or "default")
        task_id = str(getattr(args, "task_id", "dpone_gitops_task") or "dpone_gitops_task")
        self._write_base_runner_files(
            args=args,
            repo_root=repo_root,
            output_dir=output_dir,
            pod_template_path=pod_template_path,
            entrypoint_path=entrypoint_path,
            image=image,
            namespace=namespace,
            service_account=service_account,
            bundle_path=bundle_path,
            commands=commands,
        )
        run_spec = self._run_spec_builder.build(
            bundle_path=bundle_path,
            bundle=bundle,
            image=image,
            image_digest=_optional_str(getattr(args, "image_digest", None)),
            worktree=".",
            evidence_output=evidence_path.as_posix(),
            require_attestation=bool(getattr(args, "require_attestation", False)),
        )
        self._ctx.fs.write_text(repo_root / run_spec_path, run_spec.to_json(), encoding="utf-8")
        self._ctx.fs.write_text(repo_root / image_contract_path, build_image_contract(args).to_json(), encoding="utf-8")
        profile = self._runtime_profile_builder.build(
            GitOpsAirflowRuntimeProfileInput(
                bundle_path=bundle_path,
                run_spec_path=run_spec_path.as_posix(),
                runtime_evidence_path=evidence_path.as_posix(),
                xcom_summary_path=xcom_summary_path.as_posix(),
                dag_factory_path=dag_factory_path.as_posix(),
                outcome_gate_path=outcome_gate_path.as_posix(),
                image=image,
                image_digest=_optional_str(getattr(args, "image_digest", None)),
                namespace=namespace,
                service_account=service_account,
                resource_requests={"cpu": "100m", "memory": "256Mi"},
                resource_limits={"cpu": "1", "memory": "1Gi"},
                artifact_sink_kind="local",
                artifact_sink_path=output_dir.as_posix(),
                runner_policy="advisory",
                outcome_mode=_outcome_mode(args),
                bundle=bundle,
                run_spec=run_spec.to_jsonable(),
            )
        )
        xcom_summary = self._runtime_profile_builder.xcom_summary(
            profile,
            runtime_profile_path=runtime_profile_path.as_posix(),
        )
        self._ctx.fs.write_text(repo_root / runtime_profile_path, profile.to_json(), encoding="utf-8")
        self._ctx.fs.write_text(repo_root / xcom_summary_path, xcom_summary.to_json(), encoding="utf-8")
        self._ctx.fs.write_text(
            repo_root / dag_factory_path,
            self._renderer.dag_factory(
                task_id=task_id,
                image=image,
                namespace=namespace,
                service_account=service_account,
                run_spec_path=run_spec_path.as_posix(),
                runtime_profile_path=runtime_profile_path.as_posix(),
                runtime_evidence_path=evidence_path.as_posix(),
                xcom_summary_path=xcom_summary_path.as_posix(),
                outcome_mode=profile.outcome_mode,
                env=profile.env,
                labels=profile.labels or {},
                annotations=profile.annotations or {},
            ),
            encoding="utf-8",
        )
        self._ctx.fs.write_text(
            repo_root / outcome_gate_path,
            self._renderer.outcome_gate_module(upstream_task_id=task_id, required_status="passed"),
            encoding="utf-8",
        )

    def _write_base_runner_files(
        self,
        *,
        args: object,
        repo_root: Path,
        output_dir: Path,
        pod_template_path: Path,
        entrypoint_path: Path,
        image: str,
        namespace: str,
        service_account: str,
        bundle_path: str,
        commands: tuple[str, ...],
    ) -> None:
        pod_template = self._renderer.pod_template(
            image=image,
            namespace=namespace,
            service_account=service_account,
            bundle_path=bundle_path,
        )
        task_id = str(getattr(args, "task_id", "dpone_gitops_task") or "dpone_gitops_task")
        self._ctx.fs.write_text(repo_root / pod_template_path, self._ctx.yaml.dump(pod_template), encoding="utf-8")
        self._ctx.fs.write_text(
            repo_root / output_dir / "executor_config.json",
            self._renderer.executor_config(image=image, pod_template_path=pod_template_path.as_posix()),
            encoding="utf-8",
        )
        self._ctx.fs.write_text(
            repo_root / output_dir / "airflow_task.py",
            self._renderer.airflow_task(
                dag_id=str(getattr(args, "dag_id", "dpone_gitops") or "dpone_gitops"),
                task_id=task_id,
                image=image,
                namespace=namespace,
                service_account=service_account,
                pod_template_path=pod_template_path.as_posix(),
                entrypoint_path=entrypoint_path.as_posix(),
            ),
            encoding="utf-8",
        )
        self._ctx.fs.write_text(
            repo_root / entrypoint_path,
            self._renderer.entrypoint(commands=commands),
            encoding="utf-8",
        )


def _optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _outcome_mode(args: object) -> str:
    return str(getattr(args, "outcome_mode", "strict_fail") or "strict_fail").strip() or "strict_fail"
