from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path

from dpone.ports.project_authoring_lock import AuthoringLockFactory, ProjectAuthoringLockError
from dpone.services.docs.errors import DocsConfigurationError

from ...metrics.module_size import (
    ModuleSizeReport,
    ModuleSizeThresholds,
    analyze_module_sizes,
    format_module_size_report_jsonable,
    format_module_size_report_text,
)
from ...metrics.module_size_policy import (
    AUDITED_BOOTSTRAP_COMMIT,
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeGitContext,
    confined_repo_path,
    decode_module_size_baseline,
    is_legacy_empty_baseline,
    resolve_module_size_git_context,
    validate_module_size_baseline,
)
from ...metrics.module_size_snapshot import load_module_size_head_snapshot
from ...metrics.module_size_write import (
    ModuleSizeBaselineWriteOutcome,
    format_module_size_write_outcome,
    persist_baseline_candidate,
    ratchet_module_size_baseline,
)
from .context import DocsServiceContext


class CheckModuleSizeService:
    """Run CI-friendly module-size checks with explicit technical-debt allowlists."""

    def __init__(
        self,
        *,
        ctx: DocsServiceContext,
        clock: Callable[[], date] | None = None,
        authoring_lock: AuthoringLockFactory | None = None,
    ):
        self.ctx = ctx
        self.log = ctx.logger
        self._clock = clock or _utc_today
        self._authoring_lock = authoring_lock

    def run(self, args: argparse.Namespace) -> tuple[int, str | dict]:
        fmt = str(getattr(args, "format", "text") or "text").strip().lower()
        try:
            if fmt not in {"json", "text"}:
                raise DocsConfigurationError("--format must be text or json")
            package_dir = self._resolve_package(getattr(args, "package", "src/dpone"))
            thresholds = self._thresholds(args)
            baseline_path = self._resolve_baseline_path(args)
            result = self._analyze(
                args=args,
                package_dir=package_dir,
                baseline_path=baseline_path,
                thresholds=thresholds,
            )
        except (DocsConfigurationError, ModuleSizeBaselineError) as exc:
            failure_payload = {
                "schema_version": "dpone.module-size-error.v2",
                "ok": False,
                "configuration_error": str(exc),
            }
            if fmt == "text":
                return 2, f"Module size check configuration failure: {exc}\n"
            if fmt == "json":
                return 2, failure_payload
            raise DocsConfigurationError("--format must be text or json") from exc

        if isinstance(result, ModuleSizeBaselineWriteOutcome):
            return 2, format_module_size_write_outcome(result, fmt=fmt)
        report = result

        payload: str | dict = (
            format_module_size_report_jsonable(report) if fmt == "json" else format_module_size_report_text(report)
        )

        exit_code = 0 if report.ok else 2
        if exit_code:
            self.log.error("Module size check failed: %s issue(s)", report.issue_count)
        else:
            self.log.info("Module size check OK")
        return exit_code, payload

    def _analyze(
        self,
        *,
        args: argparse.Namespace,
        package_dir: Path,
        baseline_path: Path | None,
        thresholds: ModuleSizeThresholds,
    ) -> ModuleSizeReport | ModuleSizeBaselineWriteOutcome:
        base_ref = str(getattr(args, "base_ref", "") or "").strip()
        head_ref = str(getattr(args, "head_ref", "") or "").strip()
        if baseline_path is None:
            canonical_package = confined_repo_path(
                self.ctx.settings.repo_root,
                Path("src/dpone"),
                label="Canonical module-size package",
            )
            if package_dir == canonical_package:
                raise ModuleSizeBaselineError(
                    "--no-baseline cannot scan canonical package src/dpone; use the v2 baseline with exact refs"
                )
            if base_ref or head_ref:
                raise ModuleSizeBaselineError("--base-ref/--head-ref cannot be used with --no-baseline")
            self._enforce_global_hard_limits(thresholds, snapshot=None)
            if bool(getattr(args, "write_baseline", False)):
                raise ModuleSizeBaselineError("--write-baseline requires --baseline")
            return analyze_module_sizes(
                package_dir,
                repo_root=self.ctx.settings.repo_root,
                thresholds=thresholds,
                warning_debt_requires_baseline=False,
            )
        if not base_ref or not head_ref:
            raise ModuleSizeBaselineError("--base-ref and --head-ref are required with a v2 baseline")
        git_context = resolve_module_size_git_context(
            repo_root=self.ctx.settings.repo_root,
            baseline_path=baseline_path,
            base_ref=base_ref,
            head_ref=head_ref,
        )
        snapshot = load_module_size_head_snapshot(
            repo_root=self.ctx.settings.repo_root,
            package_dir=package_dir,
            baseline_path=baseline_path,
            head_sha=git_context.head_sha,
        )
        budgets = self._enforce_global_hard_limits(thresholds, snapshot=snapshot.quality_budgets)
        baseline = self._load_or_bootstrap(
            baseline_path,
            baseline_bytes=snapshot.baseline,
            package_dir=package_dir,
            thresholds=thresholds,
            git_context=git_context,
            write=bool(getattr(args, "write_baseline", False)),
            source_bytes_by_path=snapshot.source_bytes_by_path,
        )
        trusted_paths = frozenset(snapshot.source_bytes_by_path)
        accepted_adrs = snapshot.accepted_adr_text_by_path
        measured_report = analyze_module_sizes(
            package_dir,
            repo_root=self.ctx.settings.repo_root,
            thresholds=thresholds,
            baseline=baseline.entries,
            base_sha=git_context.base_sha,
            head_sha=git_context.head_sha,
            warning_debt_requires_baseline=True,
            source_bytes_by_path=snapshot.source_bytes_by_path,
        )
        current_sizes = {item.path: (item.lines, item.sloc) for item in measured_report.items}
        policy_issues = validate_module_size_baseline(
            baseline,
            repo_root=self.ctx.settings.repo_root,
            git_context=git_context,
            as_of=self._clock(),
            accepted_adr_text_by_path=accepted_adrs,
            trusted_module_paths=trusted_paths,
            current_module_sizes=current_sizes,
            warning_thresholds=(thresholds.warn_lines, thresholds.warn_sloc or 0),
            budget_limits=budgets,
        )
        report = measured_report
        if policy_issues:
            report = analyze_module_sizes(
                package_dir,
                repo_root=self.ctx.settings.repo_root,
                thresholds=thresholds,
                baseline=baseline.entries,
                policy_issues=policy_issues,
                base_sha=git_context.base_sha,
                head_sha=git_context.head_sha,
                warning_debt_requires_baseline=True,
                source_bytes_by_path=snapshot.source_bytes_by_path,
            )
        if bool(getattr(args, "write_baseline", False)):
            candidate = ratchet_module_size_baseline(
                baseline,
                report=report,
                thresholds=thresholds,
                bootstrap=self._is_legacy_empty(snapshot.baseline, source=snapshot.baseline_path),
                bootstrap_commit=AUDITED_BOOTSTRAP_COMMIT,
                git_context=git_context,
                policy_issues=policy_issues,
            )
            candidate_policy_issues = validate_module_size_baseline(
                candidate,
                repo_root=self.ctx.settings.repo_root,
                git_context=git_context,
                as_of=self._clock(),
                accepted_adr_text_by_path=accepted_adrs,
                trusted_module_paths=trusted_paths,
                current_module_sizes=current_sizes,
                warning_thresholds=(thresholds.warn_lines, thresholds.warn_sloc or 0),
                budget_limits=budgets,
            )
            candidate_report = analyze_module_sizes(
                package_dir,
                repo_root=self.ctx.settings.repo_root,
                thresholds=thresholds,
                baseline=candidate.entries,
                policy_issues=candidate_policy_issues,
                base_sha=git_context.base_sha,
                head_sha=git_context.head_sha,
                warning_debt_requires_baseline=True,
                source_bytes_by_path=snapshot.source_bytes_by_path,
            )
            if not candidate_report.ok:
                details = "; ".join(f"{issue.path}: {issue.message}" for issue in candidate_report.issues[:5])
                raise ModuleSizeBaselineError(f"Refusing to persist a non-passing baseline: {details}")
            changed = self._is_legacy_empty(snapshot.baseline, source=snapshot.baseline_path) or candidate != baseline
            if changed:
                if self._authoring_lock is None:
                    raise ModuleSizeBaselineError("Module-size baseline writer lock is not configured")
                try:
                    persist_baseline_candidate(
                        repo_root=self.ctx.settings.repo_root,
                        package_dir=package_dir,
                        baseline_path=baseline_path,
                        baseline=candidate,
                        snapshot=snapshot,
                        snapshot_loader=load_module_size_head_snapshot,
                        authoring_lock=self._authoring_lock,
                    )
                except ProjectAuthoringLockError as exc:
                    raise ModuleSizeBaselineError("Another repository authoring transaction is active") from exc
            return ModuleSizeBaselineWriteOutcome(
                changed=changed,
                baseline_path=snapshot.baseline_path,
                base_sha=git_context.base_sha,
                head_sha=git_context.head_sha,
            )
        return report

    @staticmethod
    def _thresholds(args: argparse.Namespace) -> ModuleSizeThresholds:
        warn_lines = _positive_threshold(getattr(args, "warn_lines", None), label="--warn-lines", default=450)
        max_lines = _positive_threshold(getattr(args, "max_lines", None), label="--max-lines", default=600)
        warn_sloc = _positive_threshold(getattr(args, "warn_sloc", None), label="--warn-sloc", default=350)
        max_sloc = _positive_threshold(getattr(args, "max_sloc", None), label="--max-sloc", default=400)
        if warn_lines >= max_lines:
            raise DocsConfigurationError("--warn-lines must be lower than --max-lines")
        if warn_sloc >= max_sloc:
            raise DocsConfigurationError("--warn-sloc must be lower than --max-sloc")
        return ModuleSizeThresholds(warn_lines, max_lines, warn_sloc, max_sloc)

    def _enforce_global_hard_limits(
        self, thresholds: ModuleSizeThresholds, *, snapshot: bytes | None
    ) -> tuple[int, int, int, int]:
        try:
            if snapshot is None:
                path = confined_repo_path(
                    self.ctx.settings.repo_root,
                    Path("docs/benchmarks/quality_budgets.yml"),
                    label="Quality budget path",
                )
                text = path.read_text(encoding="utf-8")
            else:
                text = snapshot.decode("utf-8")
            payload = self.ctx.yaml.load(text)
            global_budget = payload["global"]
            warn_lines = global_budget["warn_loc"]
            max_lines = global_budget["max_loc"]
            warn_sloc = global_budget["warn_sloc"]
            max_sloc = global_budget["max_sloc"]
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise ModuleSizeBaselineError("Cannot load authoritative global module-size budgets") from exc
        budgets = (warn_lines, max_lines, warn_sloc, max_sloc)
        if any(isinstance(value, bool) or not isinstance(value, int) for value in budgets):
            raise ModuleSizeBaselineError("Authoritative global module-size budgets must be integers")
        if thresholds.warn_lines > warn_lines or thresholds.warn_sloc is None or thresholds.warn_sloc > warn_sloc:
            raise ModuleSizeBaselineError(
                f"Requested warning thresholds cannot exceed authoritative warn_loc={warn_lines}, warn_sloc={warn_sloc}"
            )
        if thresholds.max_lines > max_lines or thresholds.max_sloc is None or thresholds.max_sloc > max_sloc:
            raise ModuleSizeBaselineError(
                f"Requested hard limits cannot exceed authoritative max_loc={max_lines}, max_sloc={max_sloc}"
            )
        return warn_lines, max_lines, warn_sloc, max_sloc

    def _load_or_bootstrap(
        self,
        path: Path,
        *,
        baseline_bytes: bytes,
        package_dir: Path,
        thresholds: ModuleSizeThresholds,
        git_context: ModuleSizeGitContext,
        write: bool,
        source_bytes_by_path: dict[str, bytes],
    ) -> ModuleSizeBaseline:
        if self._is_legacy_empty(baseline_bytes, source=str(path)):
            if not write or git_context.base_sha != AUDITED_BOOTSTRAP_COMMIT:
                raise ModuleSizeBaselineError(
                    "Legacy baseline is accepted only for --write-baseline from audited bootstrap "
                    f"{AUDITED_BOOTSTRAP_COMMIT}"
                )
            scan = analyze_module_sizes(
                package_dir,
                repo_root=self.ctx.settings.repo_root,
                thresholds=thresholds,
                warning_debt_requires_baseline=False,
                source_bytes_by_path=source_bytes_by_path,
            )
            hard = [issue for issue in scan.issues if "hard max" in issue.message]
            if hard:
                raise ModuleSizeBaselineError("Hard module-size violations must be split before v2 bootstrap")
            return ModuleSizeBaseline(entries=())
        return decode_module_size_baseline(baseline_bytes, source=f"{git_context.head_sha}:{path}")

    @staticmethod
    def _is_legacy_empty(raw: bytes, *, source: str) -> bool:
        return is_legacy_empty_baseline(raw, source=source)

    def _resolve_package(self, raw: object) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError("Package argument must not be empty")
        try:
            path = confined_repo_path(self.ctx.settings.repo_root, Path(text), label="Module-size package")
        except ModuleSizeBaselineError as exc:
            raise DocsConfigurationError(str(exc)) from exc
        if not path.is_dir():
            relative = path.relative_to(self.ctx.settings.repo_root.absolute()).as_posix()
            raise DocsConfigurationError(
                f"Module-size target directory does not exist: {relative}; choose an existing repository-relative directory"
            )
        return path

    def _resolve_baseline_path(self, args: argparse.Namespace) -> Path | None:
        if bool(getattr(args, "no_baseline", False)):
            return None
        baseline = str(getattr(args, "baseline", "docs/module_size_baseline.json") or "").strip()
        if not baseline:
            raise DocsConfigurationError("--baseline must not be empty; use --no-baseline for local-only mode")
        path = Path(baseline)
        try:
            return confined_repo_path(self.ctx.settings.repo_root, path, label="Baseline path")
        except ModuleSizeBaselineError as exc:
            raise DocsConfigurationError(str(exc)) from exc


def _positive_threshold(raw: object, *, label: str, default: int) -> int:
    if raw is None:
        return default
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        raise DocsConfigurationError(f"{label} must be a positive integer")
    text = str(raw).strip()
    try:
        value = int(text)
    except ValueError as exc:
        raise DocsConfigurationError(f"{label} must be a positive integer") from exc
    if not text or value <= 0:
        raise DocsConfigurationError(f"{label} must be a positive integer")
    return value


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()  # noqa: UP017 -- Python 3.10 compatibility
