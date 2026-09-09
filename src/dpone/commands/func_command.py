from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from .base import Command
from .context import CommandContext


@dataclass(frozen=True)
class FuncCommand(Command):
    """Adapter: (register_parser + handler) => OOP Command."""

    _name: str
    _register_parser: Callable[[argparse._SubParsersAction], argparse.ArgumentParser]
    _handler: Callable[..., int]
    _requires_app_context: bool = True

    @property
    def name(self) -> str:
        return self._name

    @property
    def requires_app_context(self) -> bool:
        return self._requires_app_context

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = self._register_parser(subparsers)
        p.set_defaults(_command=self)
        return p

    def run(self, args: argparse.Namespace, ctx: CommandContext | None) -> int:
        if ctx is None and self._requires_app_context:
            raise RuntimeError(f"Command `{self._name}` requires an application context")
        logger = ctx.logger if ctx is not None else logging.getLogger("dpone")
        return int(self._handler(args, ctx=ctx, logger=logger))


class CommandGroup(Command):
    """Group command (e.g. `manifest`, `dag`)."""

    def __init__(
        self,
        *,
        name: str,
        help: str,
        build_parser: Callable[[argparse._SubParsersAction], argparse.ArgumentParser],
        subcommands: Iterable[Command],
        subdest: str,
    ) -> None:
        self._name = name
        self._help = help
        self._build_parser = build_parser
        self._subcommands = list(subcommands)
        self._subdest = subdest

    @property
    def name(self) -> str:
        return self._name

    @property
    def subcommands(self) -> tuple[Command, ...]:
        """Read-only subcommand view for docs/tests/CLI introspection."""

        return tuple(self._subcommands)

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = self._build_parser(subparsers)
        sub = p.add_subparsers(dest=self._subdest, required=True)
        for cmd in self._subcommands:
            cmd.register(sub)
        return p

    def run(self, args: argparse.Namespace, ctx: CommandContext | None) -> int:
        raise RuntimeError(f"No subcommand specified for `{self._name}`")
