"""Backward-compatible sink contract facade."""

from __future__ import annotations

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.sink_protocol import AbstractSink

__all__ = ["AbstractSink", "LoadPayload", "LoadResult"]
