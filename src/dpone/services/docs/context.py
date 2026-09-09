from __future__ import annotations

import logging
from typing import Protocol

from dpone.app.settings import Settings
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec


class DocsServiceContext(Protocol):
    """Capabilities required by documentation maintenance services."""

    @property
    def settings(self) -> Settings:
        """Application settings with repository paths."""

    @property
    def logger(self) -> logging.Logger:
        """Logger used for CI-friendly diagnostics."""

    @property
    def fs(self) -> FileSystem:
        """Filesystem adapter used to read/write generated docs."""

    @property
    def yaml(self) -> YamlCodec:
        """YAML codec used for registry-backed docs checks."""


__all__ = ["DocsServiceContext"]
