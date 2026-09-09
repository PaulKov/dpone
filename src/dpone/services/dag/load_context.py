from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.app.settings import Settings
    from dpone.dag.edge_explain import DagEdgeContext
    from dpone.dag.yaml_types import ProcessNode
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

from dpone.dag.edge_explain import build_edge_context
from dpone.dag.errors import DagConfigurationError
from dpone.dag.loader import ConfigLoader
from dpone.dag.manager import DependencyManager
from dpone.manifest.loader import ManifestLoaderRouter


@dataclass(frozen=True, slots=True)
class DagCommandContext:
    """A fully loaded dpone DAG context for CLI commands."""

    base_path: Path
    root_path: Path
    registry_paths: tuple[Path, ...]

    cfg_loader: ConfigLoader
    dep_manager: DependencyManager

    nodes: tuple[ProcessNode, ...]
    edge_ctx: DagEdgeContext


class _DagLoaderContext(Protocol):
    @property
    def settings(self) -> Settings:
        """Application settings required for manifest base-path defaults."""


def resolve_entry_yaml_path(yaml_path: str, base_path: Path) -> Path:
    """Resolve a YAML path like DAG builder does.

    - If no extension, '.yaml' is assumed.
    - Relative paths are resolved against base_path.
    """

    s = str(yaml_path or "").strip()
    if not s:
        raise DagConfigurationError("Empty YAML path")

    if not s.endswith((".yaml", ".yml")):
        s = f"{s}.yaml"

    p = Path(s)
    if p.is_absolute():
        return p
    return base_path / p


def _normalize_registry_paths(raw: Sequence[str | os.PathLike[str] | None]) -> tuple[Path, ...]:
    """Parse --registry CLI arguments into absolute-ish Paths.

    We keep behaviour close to the legacy CLI:
    - do not resolve relative paths against base_path automatically
    - leave resolution to the OS / current working directory

    The only normalisation we do is:
    - strip empty strings
    - dedupe by resolved path
    """

    out: list[Path] = []
    seen: set[str] = set()

    for p in raw or []:
        if p is None:
            continue
        s = str(p).strip()
        if not s:
            continue

        path = Path(s)
        key = str(path.resolve(strict=False))
        if key in seen:
            continue
        seen.add(key)
        out.append(path)

    return tuple(out)


def load_dag_context(
    args: object,
    *,
    ctx: object,
    root_attr: str = "root",
    base_path_attr: str = "base_path",
    registry_attr: str = "registry",
) -> DagCommandContext:
    """Build a reusable DAG context from CLI args.

    This is intentionally a very small orchestration layer.

    Args:
        args: argparse.Namespace-like object.
        ctx: application context exposing settings.
        root_attr: attribute name for root manifest arg.
        base_path_attr: optional attribute name for base path arg.
        registry_attr: optional attribute name for registry args.

    Returns:
        DagCommandContext.

    Raises:
        DagConfigurationError: on missing/invalid inputs.
    """

    loader_ctx = cast(_DagLoaderContext, ctx)
    base_path_raw = getattr(args, base_path_attr, None)
    base_path = Path(str(base_path_raw)) if base_path_raw else loader_ctx.settings.manifest_dir

    root_raw = getattr(args, root_attr, None)
    root_path = resolve_entry_yaml_path(str(root_raw), base_path)

    reg_raw = getattr(args, registry_attr, None) or []
    registry_paths = _normalize_registry_paths(reg_raw)

    manifest_loader = ManifestLoaderRouter(registry_paths=registry_paths)
    cfg_loader = ConfigLoader(base_path, manifest_loader=manifest_loader)

    dm = DependencyManager(base_path, loader=cfg_loader)
    dm.load_process_chain(root_path)

    nodes = tuple(dm.get_execution_plan())
    edge_ctx = build_edge_context(nodes)

    return DagCommandContext(
        base_path=base_path,
        root_path=root_path,
        registry_paths=registry_paths,
        cfg_loader=cfg_loader,
        dep_manager=dm,
        nodes=nodes,
        edge_ctx=edge_ctx,
    )
