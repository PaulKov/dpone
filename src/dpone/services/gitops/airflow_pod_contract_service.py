from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_json_artifacts import load_gitops_json_artifact
from dpone.gitops.airflow_pod_contract import (
    GitOpsAirflowPodContractBuilder,
    GitOpsAirflowPodContractInput,
)
from dpone.gitops.airflow_pod_contract_options import (
    git_sync_option_blockers,
    parse_pod_contract_options,
)
from dpone.gitops.airflow_pod_contract_paths import resolve_pod_contract_paths
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowPodContractContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class GitOpsAirflowPodContractService:
    """Build Airflow pod contract artifacts for custom dpone runner images."""

    def __init__(self, *, ctx: GitOpsAirflowPodContractContext) -> None:
        self._ctx = ctx
        self._builder = GitOpsAirflowPodContractBuilder()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = resolve_pod_contract_paths(args)
        options, option_blockers = parse_pod_contract_options(args)
        bundle, bundle_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.bundle_path,
            label=paths.bundle_label,
            missing_code="bundle_missing",
            invalid_code="bundle_json_invalid",
            enabled=not path_blockers,
        )
        run_spec, run_spec_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.run_spec_path,
            label=paths.run_spec_label,
            missing_code="run_spec_missing",
            invalid_code="run_spec_json_invalid",
            enabled=not path_blockers,
        )
        runtime_profile, profile_blockers = _load_json(
            ctx=self._ctx,
            repo_root=repo_root,
            path=paths.runtime_profile_path,
            label=paths.runtime_profile_label,
            missing_code="runtime_profile_missing",
            invalid_code="runtime_profile_json_invalid",
            enabled=not path_blockers,
        )
        git_sync_blockers = git_sync_option_blockers(runtime_profile=_mapping(runtime_profile), options=options)
        contract = self._builder.build(
            GitOpsAirflowPodContractInput(
                bundle_path=paths.bundle_label,
                run_spec_path=paths.run_spec_label,
                runtime_profile_path=paths.runtime_profile_label,
                pod_spec_path=paths.pod_spec_label,
                kpo_kwargs_path=paths.kpo_kwargs_label,
                bundle=_mapping(bundle),
                run_spec=_mapping(run_spec),
                runtime_profile=_mapping(runtime_profile),
                image_pull_secrets=options.image_pull_secrets,
                volumes=options.volumes,
                volume_mounts=options.volume_mounts,
                env_from_configmaps=options.env_from_configmaps,
                env_secrets=options.env_secrets,
                node_selector=options.node_selector,
                tolerations=options.tolerations,
                labels=options.labels,
                annotations=options.annotations,
                on_finish_action=options.on_finish_action,
                get_logs=options.get_logs,
                deferrable=options.deferrable,
                outcome_mode=options.outcome_mode,
                blockers=(
                    *path_blockers,
                    *option_blockers,
                    *bundle_blockers,
                    *run_spec_blockers,
                    *profile_blockers,
                    *git_sync_blockers,
                ),
            )
        )
        if contract.passed:
            self._ctx.fs.write_text(repo_root / paths.output_path, contract.to_json(), encoding="utf-8")
            self._ctx.fs.write_text(
                repo_root / paths.pod_spec_path,
                self._ctx.yaml.dump(contract.pod_spec),
                encoding="utf-8",
            )
            self._ctx.fs.write_text(
                repo_root / paths.kpo_kwargs_path,
                json.dumps(contract.kpo_kwargs, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        return _view(args=args, report=contract, path=paths.output_label)


def _load_json(
    *,
    ctx: GitOpsAirflowPodContractContext,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
    enabled: bool,
) -> tuple[object, tuple[Any, ...]]:
    return load_gitops_json_artifact(
        fs=ctx.fs,
        repo_root=repo_root,
        path=path,
        label=label,
        missing_code=missing_code,
        invalid_code=invalid_code,
        source="dpone gitops airflow pod-contract",
        enabled=enabled,
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _view(*, args: object, report: Any, path: str) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_pod_contract",
            path=path,
            options={"format": getattr(args, "format", "json")},
        ),
        report=report,
    )


__all__ = ["GitOpsAirflowPodContractContext", "GitOpsAirflowPodContractService"]
