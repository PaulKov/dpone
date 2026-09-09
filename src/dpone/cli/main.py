from __future__ import annotations

import logging
import sys
from argparse import ArgumentParser
from collections.abc import Sequence
from typing import Any

from dpone.cli.public_argument_parser import PublicArgumentParser, public_error_format
from dpone.contracts.configuration_errors import ETLConfigurationError


class AppContext:
    """Lazy compatibility facade for the environment-backed composition root."""

    @staticmethod
    def from_env(*, logger: logging.Logger) -> Any:
        from ..app.context import AppContext as EnvironmentAppContext

        return EnvironmentAppContext.from_env(logger=logger)


def setup_logging() -> logging.Logger:
    """Configure production logging without importing it on hermetic CLI paths."""

    from ..app.logging import setup_logging as configure_logging

    return configure_logging()


def build_parser() -> ArgumentParser:
    """Build the complete CLI parser lazily for context-backed commands."""

    from .parser import build_parser as build_complete_parser

    return build_complete_parser()


def _build_context_free_parser(command_name: str) -> ArgumentParser:
    """Build a bounded parser for commands that must not load the runtime plane."""

    if command_name != "test":
        raise ValueError("unknown context-free command")
    from ..commands.test_cmd import cmd_test, register_parser

    class ContextFreeTestCommand:
        requires_app_context = False

        @staticmethod
        def run(args: Any, ctx: None) -> int:
            return int(cmd_test(args, ctx=ctx, logger=logging.getLogger("dpone")))

    parser = PublicArgumentParser(prog="dpone")
    subparsers = parser.add_subparsers(dest="command", required=True)
    test_parser = register_parser(subparsers)
    test_parser.set_defaults(_command=ContextFreeTestCommand())
    return parser


def _select_parser(argv: Sequence[str]) -> ArgumentParser:
    if argv and argv[0] == "test":
        return _build_context_free_parser("test")
    return build_parser()


def main(argv: list[str] | None = None) -> None:
    """Run the canonical dpone CLI.

    Parsing happens before AppContext construction so `--help` and parser errors
    remain lightweight and do not touch filesystem/env-heavy setup.
    """

    effective_argv = list(sys.argv[1:] if argv is None else argv)
    parser = _select_parser(effective_argv)
    with public_error_format(effective_argv):
        args = parser.parse_args(effective_argv)

    cmd = getattr(args, "_command", None)
    if cmd is None:
        parser.print_help()
        raise SystemExit(2)

    logger = logging.getLogger("dpone")
    ctx = None
    if getattr(cmd, "requires_app_context", True):
        logger = setup_logging()
        ctx = AppContext.from_env(logger=logger)

    try:
        code = int(cmd.run(args, ctx))
    except ETLConfigurationError as e:
        logger.error(str(e))
        raise SystemExit(2)
    except KeyboardInterrupt:
        logger.error("Interrupted")
        raise SystemExit(130)

    raise SystemExit(code)


if __name__ == "__main__":
    main(sys.argv[1:])
