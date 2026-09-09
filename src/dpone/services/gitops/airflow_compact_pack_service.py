from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, Protocol

from dpone.gitops.airflow_asset_uri import resolve_mssql_asset_registry
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder, GitOpsAirflowCompactPackReport
from dpone.gitops.airflow_dag_spec_build_models import GitOpsAirflowDagSpecBuildReport
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.airflow_reconcile_policy import (
    RECONCILE_SOURCE,
    changed_files,
    has_changed_file_selection,
    resolve_output_dir,
    resolve_output_mirror_blocker,
    validate_artifact_path,
)
from dpone.gitops.paths import GitOpsPathValidationError
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, GitOpsWorkloadCatalogReport, issue
from dpone.gitops.workload_impact import AffectedWorkloadResolver
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.airflow_compact_pack_paths import (
    dag_spec_dir as _dag_spec_dir,
)
from dpone.services.gitops.airflow_compact_pack_paths import (
    managed_artifact_root as _managed_artifact_root,
)
from dpone.services.gitops.airflow_compact_pack_paths import (
    pack_output_path as _output_path,
)
from dpone.services.gitops.airflow_compact_pack_paths import (
    pack_output_path_under as _pack_output_path,
)
from dpone.services.gitops.airflow_compact_pack_reconcile_build import build_reconcile_artifacts
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowCompactPackContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class _DagSpecArtifactWriter(Protocol):
    def validate_destination(
        self,
        report: GitOpsAirflowDagSpecBuildReport,
        *,
        artifact_dir: str | Path,
    ) -> tuple[Path, Path]: ...

    def write(
        self,
        report: GitOpsAirflowDagSpecBuildReport,
        *,
        artifact_dir: str | Path,
    ) -> GitOpsAirflowDagSpecBuildReport: ...


@dataclass(frozen=True, slots=True)
class GitOpsAirflowReconcileReport:
    workload_set: str
    env: str
    affected_workloads: tuple[str, ...]
    packs: tuple[str, ...]
    dag_specs: tuple[str, ...] = ()
    mssql_registry_identity: dict[str, str | None] | None = None
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    kind: str = "gitops.airflow_reconcile"
    schema_version: str = "1"
    producer: str = RECONCILE_SOURCE
    selection_mode: Literal["all", "affected"] = "affected"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "workload_set": self.workload_set,
            "env": self.env,
            "selection_mode": self.selection_mode,
            "affected_workloads": list(self.affected_workloads),
            "packs": list(self.packs),
            "dag_specs": list(self.dag_specs),
            "mssql_registry": self.mssql_registry_identity,
            "warnings": [item.to_jsonable() for item in self.warnings],
            "blockers": [item.to_jsonable() for item in self.blockers],
        }


class GitOpsAirflowCompactPackService:
    """Build compact workload-aware Airflow packs."""

    def __init__(
        self,
        *,
        ctx: GitOpsAirflowCompactPackContext,
        dag_spec_writer: _DagSpecArtifactWriter,
    ) -> None:
        self._ctx = ctx
        self._builder = AirflowCompactPackBuilder()
        self._dag_spec_builder = AirflowDagSpecBuilder(repo_root=ctx.settings.repo_root)
        self._dag_spec_writer = dag_spec_writer

    def pack_view(self, args: object) -> GitOpsView:
        catalog = self._resolve_catalog(args)
        registry = resolve_mssql_asset_registry(self._ctx.settings.repo_root, env=catalog.env)
        report = self._build_report(
            args=args,
            workload_id=str(getattr(args, "workload")),
            catalog=catalog,
            mssql_registry=registry,
        )
        view = GitOpsView(meta=build_gitops_meta(report.kind, path=report.output_path), report=report)
        if report.passed:
            self._write_view(view=view, output_path=report.output_path)
        return view

    def reconcile_view(self, args: object) -> GitOpsView:
        catalog = self._resolve_catalog(args)
        all_workloads = bool(getattr(args, "all_workloads", False))
        selection_mode: Literal["all", "affected"] = "all" if all_workloads else "affected"
        input_blockers: list[GitOpsWorkloadCatalogIssue] = [*catalog.blockers]
        if all_workloads and has_changed_file_selection(args):
            input_blockers.append(
                issue(
                    code="reconcile_selection_conflict",
                    message="--all-workloads cannot be combined with changed-file selection",
                    path=catalog.workload_set,
                    source=RECONCILE_SOURCE,
                )
            )

        output_dir, output_blocker = resolve_output_dir(
            args,
            repo_root=self._ctx.settings.repo_root,
        )
        if output_blocker is not None:
            input_blockers.append(output_blocker)
        output_mirror_blocker = resolve_output_mirror_blocker(
            args,
            repo_root=self._ctx.settings.repo_root,
            reserved_paths=(_managed_artifact_root(output_dir),) if output_blocker is None else (),
        )
        if output_mirror_blocker is not None:
            input_blockers.append(output_mirror_blocker)
        if input_blockers:
            return self._reconcile_view(
                catalog=catalog,
                selection_mode=selection_mode,
                blockers=tuple(input_blockers),
                output_dir=output_dir if output_blocker is None else None,
            )

        if all_workloads:
            selected_ids = tuple(workload.workload_id for workload in catalog.workloads)
            impact_warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
            impact_blockers: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
        else:
            selected_files, changed_file_blockers = changed_files(
                fs=self._ctx.fs,
                repo_root=self._ctx.settings.repo_root,
                args=args,
            )
            if changed_file_blockers:
                return self._reconcile_view(
                    catalog=catalog,
                    selection_mode=selection_mode,
                    blockers=changed_file_blockers,
                    output_dir=output_dir,
                )
            impact = AffectedWorkloadResolver(repo_root=self._ctx.settings.repo_root).resolve(
                catalog, changed_files=selected_files
            )
            selected_ids = tuple(item.workload_id for item in impact.affected_workloads)
            impact_warnings = impact.warnings
            impact_blockers = impact.blockers
        blockers: list[GitOpsWorkloadCatalogIssue] = [
            *catalog.blockers,
            *impact_blockers,
        ]
        if blockers:
            return self._reconcile_view(
                catalog=catalog,
                selection_mode=selection_mode,
                selected_ids=selected_ids,
                warnings=impact_warnings,
                blockers=tuple(blockers),
                output_dir=output_dir,
            )

        registry = resolve_mssql_asset_registry(self._ctx.settings.repo_root, env=catalog.env)
        built = build_reconcile_artifacts(
            catalog=catalog,
            selected_ids=selected_ids,
            mssql_registry=registry,
            dag_spec_builder=self._dag_spec_builder,
            build_pack=lambda workload_id, mssql_registry: self._build_report(
                args=args,
                workload_id=workload_id,
                output_dir=output_dir,
                catalog=catalog,
                mssql_registry=mssql_registry,
            ),
            repo_root=self._ctx.settings.repo_root,
        )
        pack_reports = list(built.pack_reports)
        pack_warnings = built.pack_warnings
        blockers.extend(built.blockers)
        dag_spec_report = built.dag_spec_report
        if dag_spec_report is None or blockers:
            return self._reconcile_view(
                catalog=catalog,
                selection_mode=selection_mode,
                selected_ids=selected_ids,
                warnings=(*impact_warnings, *pack_warnings, *(dag_spec_report.warnings if dag_spec_report else ())),
                blockers=tuple(blockers),
                output_dir=output_dir,
            )
        blockers.extend(self._output_path_blockers(pack_reports))
        blockers.extend(self._dag_spec_path_blockers(dag_spec_report, output_dir=output_dir))
        if blockers:
            return self._reconcile_view(
                catalog=catalog,
                selection_mode=selection_mode,
                selected_ids=selected_ids,
                warnings=(*impact_warnings, *pack_warnings, *dag_spec_report.warnings),
                blockers=tuple(blockers),
                output_dir=output_dir,
            )

        for report in pack_reports:
            self._write_report(report)
        written_dag_specs = self._dag_spec_writer.write(
            dag_spec_report,
            artifact_dir=_dag_spec_dir(output_dir),
        )
        reconcile = GitOpsAirflowReconcileReport(
            workload_set=catalog.workload_set,
            env=catalog.env,
            affected_workloads=selected_ids,
            packs=tuple(report.output_path for report in pack_reports),
            dag_specs=written_dag_specs.output_paths,
            mssql_registry_identity=registry.identity(),
            warnings=(
                *catalog.warnings,
                *impact_warnings,
                *pack_warnings,
                *written_dag_specs.warnings,
            ),
            selection_mode=selection_mode,
        )
        return GitOpsView(
            meta=build_gitops_meta(reconcile.kind, path=output_dir.as_posix()),
            report=reconcile,
        )

    def _reconcile_view(
        self,
        *,
        catalog: GitOpsWorkloadCatalogReport,
        selection_mode: Literal["all", "affected"],
        selected_ids: tuple[str, ...] = (),
        warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = (),
        blockers: tuple[GitOpsWorkloadCatalogIssue, ...],
        output_dir: Path | None = None,
    ) -> GitOpsView:
        reconcile = GitOpsAirflowReconcileReport(
            workload_set=catalog.workload_set,
            env=catalog.env,
            affected_workloads=selected_ids,
            packs=(),
            warnings=(*catalog.warnings, *warnings),
            blockers=blockers,
            selection_mode=selection_mode,
        )
        return GitOpsView(
            meta=build_gitops_meta(
                reconcile.kind,
                path=(output_dir.as_posix() if output_dir is not None else ""),
            ),
            report=reconcile,
        )

    def _build_report(
        self,
        *,
        args: object,
        workload_id: str,
        output_dir: Path | None = None,
        catalog: GitOpsWorkloadCatalogReport | None = None,
        mssql_registry=None,
    ) -> GitOpsAirflowCompactPackReport:
        resolved = catalog or self._resolve_catalog(args)
        registry = mssql_registry or resolve_mssql_asset_registry(self._ctx.settings.repo_root, env=resolved.env)
        output_path = (
            _pack_output_path(output_dir, workload_id) if output_dir is not None else _output_path(args, workload_id)
        )
        try:
            workload = resolved.by_id(workload_id)
        except KeyError:
            missing = issue(code="workload_not_found", message="GitOps workload id was not found", path=workload_id)
            placeholder = resolved.workloads[0] if resolved.workloads else None
            if placeholder is None:
                raise
            report = self._builder.build(workload=placeholder, output_path=output_path)
            return replace(report, blockers=(missing,))
        return self._builder.build(
            workload=workload,
            output_path=output_path,
            repo_root=self._ctx.settings.repo_root,
            mode=str(getattr(args, "mode", "plan")),
            runner_policy=getattr(args, "runner_policy", None),
            include_live_gates=bool(getattr(args, "include_live_gates", False)),
            env=resolved.env,
            mssql_registry=registry,
        )

    def _resolve_catalog(self, args: object) -> GitOpsWorkloadCatalogReport:
        return WorkloadCatalogResolver(repo_root=self._ctx.settings.repo_root).resolve(
            getattr(args, "workload_set"), env=str(getattr(args, "env", "dev"))
        )

    def _write_report(self, report: GitOpsAirflowCompactPackReport) -> None:
        self._ctx.fs.write_text(self._ctx.settings.repo_root / report.output_path, report.to_json(), encoding="utf-8")

    def _output_path_blockers(
        self,
        reports: list[GitOpsAirflowCompactPackReport],
    ) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
        blockers: list[GitOpsWorkloadCatalogIssue] = []
        for report in reports:
            blocker = validate_artifact_path(
                repo_root=self._ctx.settings.repo_root,
                raw_path=report.output_path,
            )
            if blocker is not None:
                blockers.append(blocker)
        return tuple(blockers)

    def _dag_spec_path_blockers(
        self,
        report: GitOpsAirflowDagSpecBuildReport,
        *,
        output_dir: Path,
    ) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
        try:
            self._dag_spec_writer.validate_destination(
                report,
                artifact_dir=_dag_spec_dir(output_dir),
            )
        except GitOpsPathValidationError as exc:
            return (
                issue(
                    code="invalid_path",
                    message=str(exc),
                    path=_dag_spec_dir(output_dir),
                    source=RECONCILE_SOURCE,
                ),
            )
        return ()

    def _write_view(self, *, view: GitOpsView, output_path: str) -> None:
        import json

        payload = json.dumps(view.to_jsonable(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        self._ctx.fs.write_text(self._ctx.settings.repo_root / output_path, payload, encoding="utf-8")


__all__ = ["GitOpsAirflowCompactPackContext", "GitOpsAirflowCompactPackService", "GitOpsAirflowReconcileReport"]
