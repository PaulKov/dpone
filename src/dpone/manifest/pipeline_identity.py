"""Canonical lexical identity for one dpone pipeline."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

PIPELINE_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{1,127}$"
_PIPELINE_ID_PATTERN = re.compile(PIPELINE_ID_PATTERN)
_NONCANONICAL_RUN = re.compile(r"[^a-z0-9_-]+")
_LEADING_NONALNUM = re.compile(r"^[^a-z0-9]+")
_FALLBACK_ID = "pipeline"


class PipelineIdError(ValueError):
    """A public pipeline identity does not match the canonical schema."""

    def __init__(self, *, suggested_id: str) -> None:
        super().__init__(f"Pipeline id must match {PIPELINE_ID_PATTERN}.")
        self.suggested_id = suggested_id


@dataclass(frozen=True, slots=True)
class PipelineId:
    """Validated pipeline identity shared by authoring and lookup surfaces."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or _PIPELINE_ID_PATTERN.fullmatch(self.value) is None:
            raise PipelineIdError(suggested_id=self.suggest(self.value))

    @classmethod
    def parse(cls, value: str) -> PipelineId:
        if not isinstance(value, str) or _PIPELINE_ID_PATTERN.fullmatch(value) is None:
            raise PipelineIdError(suggested_id=cls.suggest(value))
        return cls(value=value)

    @staticmethod
    def suggest(value: str) -> str:
        """Return a deterministic canonical suggestion without changing user input."""

        normalized = unicodedata.normalize("NFKD", value if isinstance(value, str) else "")
        ascii_value = normalized.encode("ascii", "ignore").decode("ascii").strip().lower()
        candidate = _NONCANONICAL_RUN.sub("_", ascii_value)
        candidate = _LEADING_NONALNUM.sub("", candidate)[:128]
        if not candidate:
            return _FALLBACK_ID
        if len(candidate) == 1:
            candidate = f"{candidate}_pipeline"
        return candidate if _PIPELINE_ID_PATTERN.fullmatch(candidate) is not None else _FALLBACK_ID

    def __str__(self) -> str:
        return self.value


__all__ = ["PIPELINE_ID_PATTERN", "PipelineId", "PipelineIdError"]
