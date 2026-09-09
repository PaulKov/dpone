from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_models import GitOpsAirflowImageContract, GitOpsAirflowImageContractReport
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowImageContractContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowImageContractService:
    """Builds the custom dpone image contract used by Airflow runner gates."""

    def __init__(self, *, ctx: GitOpsAirflowImageContractContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        output_path, output_label, path_blocker = _resolve_output_path(args)
        contract = build_image_contract(args)
        blockers = tuple(
            issue
            for issue in (
                path_blocker,
                _image_blocker(contract.image),
            )
            if issue is not None
        )
        report = GitOpsAirflowImageContractReport(output_path=output_label, contract=contract, blockers=blockers)
        if report.passed:
            self._ctx.fs.write_text(repo_root / output_path, contract.to_json(), encoding="utf-8")
        return _view(args=args, report=report)


def build_image_contract(args: object) -> GitOpsAirflowImageContract:
    tools = _tools(getattr(args, "tool", None))
    return GitOpsAirflowImageContract(
        image=str(getattr(args, "image", "") or "").strip(),
        image_digest=_optional_str(getattr(args, "image_digest", None)),
        dpone_version=_optional_str(getattr(args, "dpone_version", None)),
        python_version=_optional_str(getattr(args, "python_version", None)),
        airflow_provider_version=_optional_str(getattr(args, "airflow_provider_version", None)),
        tools=tools,
        user=_optional_str(getattr(args, "user", None)),
        workdir=_optional_str(getattr(args, "workdir", None)),
        entrypoint=_optional_str(getattr(args, "entrypoint", None)),
    )


def _resolve_output_path(args: object) -> tuple[Path, str, GitOpsIssue | None]:
    raw_output_path = getattr(args, "output_path", ".dpone/gitops/airflow/image-contract.json")
    try:
        output_path = safe_relative_path(raw_output_path, source="--output-path")
    except GitOpsPathValidationError as exc:
        return (
            Path("."),
            str(raw_output_path),
            GitOpsIssue(code="invalid_path", message=str(exc), path=str(raw_output_path), source="--output-path"),
        )
    return output_path, output_path.as_posix(), None


def _image_blocker(image: str) -> GitOpsIssue | None:
    if image:
        return None
    return GitOpsIssue(
        code="image_required",
        message="Airflow image contract requires --image",
        path="--image",
        source="dpone gitops airflow image-contract",
    )


def _tools(raw_tools: object) -> tuple[str, ...]:
    if not raw_tools:
        return ("dpone",)
    if isinstance(raw_tools, str):
        raw_iterable = (raw_tools,)
    else:
        raw_iterable = tuple(raw_tools) if isinstance(raw_tools, list | tuple) else (str(raw_tools),)
    tools: list[str] = []
    for raw_tool in raw_iterable:
        tool = str(raw_tool or "").strip()
        if tool and tool not in tools:
            tools.append(tool)
    return tuple(tools or ("dpone",))


def _optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _view(*, args: object, report: GitOpsAirflowImageContractReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_image_contract",
            path=report.output_path,
            options={"format": getattr(args, "format", "json")},
        ),
        report=report,
    )


__all__ = [
    "GitOpsAirflowImageContractContext",
    "GitOpsAirflowImageContractService",
    "build_image_contract",
]
