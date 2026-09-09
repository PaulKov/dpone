from __future__ import annotations

import argparse
from abc import ABC, abstractmethod

from .context import CommandContext


class Command(ABC):
    """OOP command contract.

    Why:
      - keep CLI thin
      - move business logic to services
      - improve testability
    """

    @property
    @abstractmethod
    def name(self) -> str:  # pragma: no cover
        raise NotImplementedError

    @property
    def requires_app_context(self) -> bool:
        """Whether the command needs the environment-backed composition root."""

        return True

    @abstractmethod
    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:  # pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def run(self, args: argparse.Namespace, ctx: CommandContext | None) -> int:  # pragma: no cover
        raise NotImplementedError
