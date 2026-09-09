"""Simple render helpers for managed command output."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class ManagedRenderer:
    @staticmethod
    def render_text(title: str, payload: Mapping[str, Any]) -> str:
        lines = [title]
        for key, value in payload.items():
            lines.append(f"- {key}: {value}")
        return "\n".join(lines) + "\n"

    @staticmethod
    def render_markdown(title: str, payload: Mapping[str, Any]) -> str:
        lines = [f"# {title}", ""]
        for key, value in payload.items():
            lines.append(f"- {key}: `{value}`")
        return "\n".join(lines) + "\n"


__all__ = ["ManagedRenderer"]
