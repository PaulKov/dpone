"""Backward-compatible source contract facade."""

from __future__ import annotations

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource

__all__ = ["AbstractSource", "ExtractResult"]
