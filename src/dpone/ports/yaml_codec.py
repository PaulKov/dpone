from __future__ import annotations

from typing import Any, Protocol


class YamlCodec(Protocol):
    """YAML serialization port."""

    def load(self, text: str) -> Any: ...

    def dump(self, obj: Any) -> str: ...
