from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.app.settings import Settings
    from dpone.ports.filesystem import FileSystem
    from dpone.ports.yaml_codec import YamlCodec


class CommandContext(Protocol):
    """Minimal CLI command context required by command adapters."""

    @property
    def logger(self) -> logging.Logger:
        """Logger used by generic command adapters."""


class DocsCommandContext(CommandContext, Protocol):
    """CLI context capabilities required by documentation commands."""

    @property
    def settings(self) -> Settings:
        """Application settings with repository paths."""

    @property
    def fs(self) -> FileSystem:
        """Filesystem adapter used to read/write generated docs."""

    @property
    def yaml(self) -> YamlCodec:
        """YAML codec used for registry-backed docs checks."""


__all__ = ["CommandContext", "DocsCommandContext"]
