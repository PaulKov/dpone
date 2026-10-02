"""Shared injected I/O capabilities for GitOps application services.

Contexts describe structural dependencies, never construct clients or perform I/O.
Service-specific contexts inherit the smallest required capability and keep their
own settings and other dependencies. Canonical port aliases are retained for
compatibility with service module imports and annotation introspection.
"""

from typing import Protocol

from dpone.ports.filesystem import FileSystem as FileSystem
from dpone.ports.yaml_codec import YamlCodec as YamlCodec


class GitOpsFileContext(Protocol):
    """A caller-owned filesystem; services must not replace it implicitly."""

    fs: FileSystem


class GitOpsYamlContext(GitOpsFileContext, Protocol):
    """Filesystem plus an injected YAML codec, with no vendor dependency."""

    yaml: YamlCodec


__all__ = ["GitOpsFileContext", "GitOpsYamlContext", "FileSystem", "YamlCodec"]
