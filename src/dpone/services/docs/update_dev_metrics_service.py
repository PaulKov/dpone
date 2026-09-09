from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dpone.services.docs.errors import DocsConfigurationError

from ...metrics.import_graph import collect_internal_deps, compute_coupling_stats
from ...metrics.layer_metrics import compute_layer_coupling_stats
from ...metrics.loc import calc_loc_stats, iter_py_files
from ...metrics.render import build_metrics_block_md
from .context import DocsServiceContext

MD_START = "<!-- DPONE_QUALITY_METRICS_START -->"
MD_END = "<!-- DPONE_QUALITY_METRICS_END -->"


@dataclass(frozen=True)
class GeneratedDoc:
    path: Path
    content: str


def _replace_block(existing: str, *, start: str, end: str, new_block: str) -> str:
    """Replace a section between markers.

    If markers do not exist, append the block to the end.
    """

    if start in existing and end in existing:
        before = existing.split(start)[0]
        after = existing.split(end, 1)[1]
        return before + new_block + after
    if not existing.endswith("\n"):
        existing += "\n"
    return existing + "\n" + new_block


class UpdateDevMetricsService:
    """Update the auto-generated quality metrics dashboard."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace) -> int:
        docs_dir = Path(str(getattr(args, "docs_dir", "docs") or "docs"))
        target = str(getattr(args, "target", "all") or "all").strip().lower()
        check = bool(getattr(args, "check", False))
        top_n = int(getattr(args, "top", 15) or 15)
        if top_n < 0:
            raise DocsConfigurationError("--top must be >= 0")

        root = self.ctx.settings.repo_root
        pkg_dir = (root / "src" / "dpone").resolve()
        if not pkg_dir.exists():
            raise DocsConfigurationError("Cannot find src/dpone/ in repo root")

        repo_py = list(iter_py_files(root, tracked_only=True, repo_root=root))
        repo_py_without_tests = [path for path in repo_py if "tests" not in path.relative_to(root).parts]
        dpone_py = list(iter_py_files(pkg_dir, tracked_only=True, repo_root=root))

        repo_loc = calc_loc_stats(repo_py, root=root, top_n=top_n)
        repo_without_tests_loc = calc_loc_stats(repo_py_without_tests, root=root, top_n=top_n)
        dpone_loc = calc_loc_stats(dpone_py, root=root, top_n=top_n)

        deps_out = collect_internal_deps(pkg_dir, module_files=dpone_py, package_name="dpone")
        coupling = compute_coupling_stats(deps_out, top_n=max(5, min(20, top_n)), package_name="dpone")
        layer_stats = compute_layer_coupling_stats(
            deps_out,
            top_n=max(5, min(20, top_n)),
            package_name="dpone",
            exclude_layers=("dpone.compat",),
        )

        def build_md() -> str:
            return build_metrics_block_md(
                start_marker=MD_START,
                end_marker=MD_END,
                repo_loc=repo_loc,
                repo_without_tests_loc=repo_without_tests_loc,
                dpone_loc=dpone_loc,
                coupling=coupling,
                layer_stats=layer_stats,
            )

        docs: list[GeneratedDoc] = []
        if target in {"all", "md", "markdown", "quality"}:
            docs.append(
                self._update_one(
                    path=(root / docs_dir / "quality-metrics.md").resolve(),
                    build_block=build_md,
                )
            )

        if not docs:
            raise DocsConfigurationError(f"Unknown target={target!r}. Allowed: all|md|quality")

        mismatched: list[Path] = []
        for d in docs:
            if check:
                if not self.ctx.fs.exists(d.path):
                    mismatched.append(d.path)
                    continue
                if self.ctx.fs.read_text(d.path) != d.content:
                    mismatched.append(d.path)
            else:
                self.ctx.fs.write_text(d.path, d.content)

        if check:
            if mismatched:
                self.log.error(
                    "Quality metrics docs are outdated (%s). Run: dpone docs update-dev-metrics",
                    ", ".join(str(p) for p in mismatched),
                )
                return 2
            self.log.info("Quality metrics docs are up-to-date.")
            return 0

        for d in docs:
            self.log.info("✅ Updated metrics: %s", d.path.as_posix())
        return 0

    def _update_one(self, *, path: Path, build_block: Callable[[], str]) -> GeneratedDoc:
        if not self.ctx.fs.exists(path):
            raise DocsConfigurationError(f"Docs file not found: {path}")
        existing = self.ctx.fs.read_text(path)
        block = build_block()
        content = _replace_block(existing, start=MD_START, end=MD_END, new_block=block)
        return GeneratedDoc(path=path, content=content)
