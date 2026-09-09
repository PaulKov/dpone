"""Deprecated compatibility shim for the old CLI module.

`dpone.cli.legacy` used to contain a very large monolithic argparse-based CLI.
All command implementations now live in:

- ``dpone.cli.main`` / ``dpone.cli.parser``
- ``dpone.commands.*``
- ``dpone.services.*``
- ``dpone.cli_render.*``

This module stays only to preserve compatibility for code that still imports
``dpone.cli.legacy.main`` or executes ``python -m dpone.cli.legacy``.
"""

from __future__ import annotations

import argparse
import sys
import warnings
from typing import Final

from .main import main as _main
from .parser import build_parser as _build_parser

_DEPRECATION_MESSAGE: Final[str] = (
    "`dpone.cli.legacy` is deprecated; use `dpone.cli.main` / `dpone` instead. "
    "The old monolithic CLI module has been replaced with commands/services/renderers."
)


_REMOVED_EXPORTS: Final[dict[str, str]] = {
    "run_legacy": "Use `dpone` / `dpone.cli.main.main(...)` or specific command modules.",
    "_cmd_manifest_list": "Manifest commands now live in `dpone.commands.manifest.*`.",
    "_cmd_manifest_render": "Manifest commands now live in `dpone.commands.manifest.*`.",
    "_cmd_manifest_explain": "Manifest commands now live in `dpone.commands.manifest.*`.",
    "_cmd_manifest_validate": "Manifest commands now live in `dpone.commands.manifest.*`.",
    "_cmd_manifest_migrate": "Manifest commands now live in `dpone.commands.manifest.*`.",
    "_cmd_manifest_verify": "Manifest commands now live in `dpone.commands.manifest.*`.",
    "_cmd_dag_explain_edge": "DAG commands now live in `dpone.commands.dag.*`.",
    "_cmd_dag_explain_edge_e2e": "DAG commands now live in `dpone.commands.dag.*`.",
    "_cmd_dag_explain_node": "DAG commands now live in `dpone.commands.dag.*`.",
    "_cmd_dag_explain_node_e2e": "DAG commands now live in `dpone.commands.dag.*`.",
    "_cmd_dag_list_edges": "DAG commands now live in `dpone.commands.dag.*`.",
    "_cmd_dag_subgraph": "DAG commands now live in `dpone.commands.dag.*`.",
    "_cmd_dag_report": "DAG commands now live in `dpone.commands.dag.*`.",
}


def _warn() -> None:
    warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)


def build_parser() -> argparse.ArgumentParser:
    """Return the canonical parser, but emit a deprecation warning."""

    _warn()
    return _build_parser()


def main(argv: list[str] | None = None) -> None:
    """Run the canonical CLI through the deprecated legacy entrypoint."""

    _warn()
    _main(argv)


def __getattr__(name: str):
    if name in _REMOVED_EXPORTS:
        raise AttributeError(f"`dpone.cli.legacy.{name}` was removed. {_REMOVED_EXPORTS[name]}")
    raise AttributeError(name)


if __name__ == "__main__":
    main(sys.argv[1:])
