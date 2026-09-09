"""Verification utilities for Variant C migration (Step 7).

Goal: provide a deterministic way to confirm that a set of *batch manifests*
compiles into the same effective per-process configs as the original legacy
(single) manifests.

This is intended for CI and safe migrations:
- compare legacy YAML (1 file = 1 process)
- against batch YAML (1 file = N processes)

The verification uses the same mapping logic as migration (plan_migration):
legacy file path -> (batch file path, selector).

By default we compare the full compiled process dicts, including depends_on.
To compare depends_on fairly, legacy depends_on is rewritten into the new
batch reference format ("file.batch.yaml#selector" and "#selector").

Design goals:
- KISS: small surface area, predictable output
- DRY: reuse migration mapping and dependency rewrite helpers
- SOLID: verification is separate from compilation/execution
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.migrate import (
    MigrationConfig,
    normalize_depends_on,
    plan_migration,
    rewrite_depends_on,
)


@dataclass(frozen=True, slots=True)
class DiffEntry:
    path: str
    legacy: Any
    batch: Any


@dataclass(frozen=True, slots=True)
class VerificationIssue:
    code: str
    message: str
    legacy_path: Path
    batch_ref: str | None = None
    selector: str | None = None
    diffs: tuple[DiffEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class VerificationReport:
    total_legacy: int
    ok: int
    failed: int
    issues: tuple[VerificationIssue, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "total_legacy": self.total_legacy,
            "ok": self.ok,
            "failed": self.failed,
            "issues": [
                {
                    **{k: v for k, v in asdict(i).items() if k != "diffs"},
                    "legacy_path": str(i.legacy_path),
                    "diffs": [asdict(d) for d in i.diffs],
                }
                for i in self.issues
            ],
        }


@dataclass(frozen=True, slots=True)
class VerificationConfig:
    legacy_src: Path
    batch_dir: Path
    recursive: bool = True
    group_by: str = "dataset"  # dataset|task_group|dataset_task_group
    compare_depends_on: bool = True
    ignore_paths: Sequence[str] = ()
    max_diffs: int = 30
    registry_paths: tuple[Path, ...] = ()  # optional registry paths for loading batch manifests


_MISSING = object()


def verify_legacy_vs_batch(cfg: VerificationConfig) -> VerificationReport:
    """Verifies that batch manifests match legacy manifests.

    Returns a report. If report.failed > 0, verification should be considered failed.
    """

    mig_cfg = MigrationConfig(
        src_path=cfg.legacy_src,
        out_dir=cfg.batch_dir,
        recursive=cfg.recursive,
        group_by=cfg.group_by,
        overwrite=False,
        dry_run=True,
        infer_naming=False,
    )
    plan = plan_migration(mig_cfg)

    legacy_processes = [p for b in plan.batches for p in b.processes]
    if not legacy_processes:
        raise ManifestConfigurationError(f"Не найдено legacy манифестов в {cfg.legacy_src}")

    loader = ManifestLoaderRouter(registry_paths=cfg.registry_paths)
    batch_cache: dict[Path, Any] = {}

    issues: list[VerificationIssue] = []
    ok = 0

    ignore = tuple(str(p).strip() for p in cfg.ignore_paths if str(p).strip())

    for legacy_proc in legacy_processes:
        legacy_path = legacy_proc.path.resolve()
        ref = plan.mapping.get(legacy_path)
        if not ref:
            issues.append(
                VerificationIssue(
                    code="MAPPING_MISSING",
                    message="Не удалось построить mapping legacy->batch (внутренняя ошибка планировщика миграции)",
                    legacy_path=legacy_path,
                )
            )
            continue

        batch_path = _resolve_batch_path(ref.batch_path, cfg.batch_dir)
        batch_ref_str = f"{batch_path.name}#{ref.selector}"

        if not batch_path.exists():
            issues.append(
                VerificationIssue(
                    code="BATCH_FILE_MISSING",
                    message=f"Batch файл не найден: {batch_path}",
                    legacy_path=legacy_path,
                    batch_ref=batch_ref_str,
                    selector=ref.selector,
                )
            )
            continue

        if batch_path not in batch_cache:
            batch_cache[batch_path] = loader.load(batch_path, metadata_only=True)

        batch_manifest = batch_cache[batch_path]
        batch_spec = _find_process(batch_manifest.processes, selector=ref.selector)
        if not batch_spec:
            issues.append(
                VerificationIssue(
                    code="BATCH_PROCESS_MISSING",
                    message=f"Process selector '{ref.selector}' не найден в batch манифесте",
                    legacy_path=legacy_path,
                    batch_ref=batch_ref_str,
                    selector=ref.selector,
                )
            )
            continue

        legacy_raw = dict(legacy_proc.raw)
        batch_raw = dict(batch_spec.raw_config)

        # Normalize depends_on for a fair comparison: rewrite legacy refs into batch refs.
        if cfg.compare_depends_on:
            deps = normalize_depends_on(legacy_raw.get("depends_on"))
            deps = rewrite_depends_on(
                deps,
                current_batch_path=batch_path,
                mapping=plan.mapping,
                source_manifest_dir=legacy_path.parent,
            )
            if deps:
                legacy_raw["depends_on"] = deps
            else:
                legacy_raw.pop("depends_on", None)

            # sort depends_on for stability
            legacy_raw = _normalize_for_compare(legacy_raw)
            batch_raw = _normalize_for_compare(batch_raw)
        else:
            legacy_raw.pop("depends_on", None)
            batch_raw.pop("depends_on", None)

        diffs = deep_diff(legacy_raw, batch_raw, ignore_paths=ignore)

        if diffs:
            # limit output to keep logs readable
            limited = tuple(diffs[: max(cfg.max_diffs, 1)])
            msg = f"Конфиг процесса отличается от legacy (diffs={len(diffs)})"
            if len(diffs) > len(limited):
                msg += f"; показаны первые {len(limited)}"
            issues.append(
                VerificationIssue(
                    code="MISMATCH",
                    message=msg,
                    legacy_path=legacy_path,
                    batch_ref=batch_ref_str,
                    selector=ref.selector,
                    diffs=limited,
                )
            )
        else:
            ok += 1

    total = len(legacy_processes)
    failed = len(issues)
    return VerificationReport(total_legacy=total, ok=ok, failed=failed, issues=tuple(issues))


def _resolve_batch_path(planned: Path, batch_dir: Path) -> Path:
    """Maps planned batch path to actual batch_dir.

    Migration plan stores absolute paths (resolved). For verification we want to
    allow passing a different batch_dir; if planned path is inside batch_dir
    already - use it; otherwise, resolve by filename within batch_dir.
    """
    planned = planned.resolve()
    batch_dir = batch_dir.resolve()
    try:
        planned.relative_to(batch_dir)
        return planned
    except Exception:
        return batch_dir / planned.name


def _find_process(processes: Sequence[Any], *, selector: str) -> Any | None:
    for spec in processes:
        if spec.selector == selector or spec.name == selector:
            return spec
    return None


def _normalize_for_compare(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Normalizes a raw config dict for stable comparison."""
    out = dict(cfg)
    deps = out.get("depends_on")
    if isinstance(deps, list):
        out["depends_on"] = _sort_depends(deps)
    return out


def _sort_depends(deps: Sequence[Any]) -> list[Any]:
    def key(item: Any) -> str:
        if isinstance(item, str):
            return f"path:{item}"
        if isinstance(item, Mapping):
            if item.get("group"):
                return f"group:{item.get('group')}"
            if item.get("path"):
                return f"path:{item.get('path')}"
            return "map:" + ",".join(sorted(map(str, item.keys())))
        return str(item)

    return sorted([_normalize_dep_item(i) for i in deps], key=key)


def _normalize_dep_item(item: Any) -> Any:
    # normalize shortcut string deps into dicts to make strict compare more robust
    if isinstance(item, str):
        return {"path": item}
    if isinstance(item, Mapping):
        return dict(item)
    return item


def deep_diff(a: Any, b: Any, *, ignore_paths: Sequence[str] = ()) -> list[DiffEntry]:
    """Returns a list of differences between two YAML-compatible objects."""

    diffs: list[DiffEntry] = []
    ignore = tuple(p for p in ignore_paths if p)

    def is_ignored(path: str) -> bool:
        for pref in ignore:
            if path == pref:
                return True
            if path.startswith(pref + "."):
                return True
            if path.startswith(pref + "["):
                return True
        return False

    def walk(x: Any, y: Any, path: str) -> None:
        if path and is_ignored(path):
            return

        if isinstance(x, Mapping) and isinstance(y, Mapping):
            keys = sorted({*x.keys(), *y.keys()}, key=lambda k: str(k))
            for k in keys:
                p = f"{path}.{k}" if path else str(k)
                xv = x.get(k, _MISSING)
                yv = y.get(k, _MISSING)
                if xv is _MISSING:
                    diffs.append(DiffEntry(path=p, legacy="<missing>", batch=yv))
                    continue
                if yv is _MISSING:
                    diffs.append(DiffEntry(path=p, legacy=xv, batch="<missing>"))
                    continue
                walk(xv, yv, p)
            return

        if isinstance(x, list) and isinstance(y, list):
            if len(x) != len(y):
                diffs.append(DiffEntry(path=f"{path} (len)", legacy=len(x), batch=len(y)))
            for i in range(min(len(x), len(y))):
                walk(x[i], y[i], f"{path}[{i}]" if path else f"[{i}]")
            return

        if x != y:
            diffs.append(DiffEntry(path=path or "<root>", legacy=x, batch=y))

    walk(a, b, "")
    return diffs
