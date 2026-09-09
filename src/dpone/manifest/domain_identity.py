"""Canonical lexical identity for one self-service domain."""

from __future__ import annotations

import re
from dataclasses import dataclass

from dpone.manifest.pipeline_identity import PIPELINE_ID_PATTERN, PipelineId

_DOMAIN_ID_PATTERN = re.compile(PIPELINE_ID_PATTERN)


class DomainIdError(ValueError):
    """A public domain identity does not match the canonical project grammar."""

    def __init__(self, *, suggested_id: str) -> None:
        super().__init__(f"Domain id must match {PIPELINE_ID_PATTERN}.")
        self.suggested_id = suggested_id


@dataclass(frozen=True, slots=True)
class DomainId:
    """Validated domain identity; invalid input is never silently rewritten."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or _DOMAIN_ID_PATTERN.fullmatch(self.value) is None:
            raise DomainIdError(suggested_id=PipelineId.suggest(self.value))

    @classmethod
    def parse(cls, value: str) -> DomainId:
        try:
            return cls(value=value)
        except DomainIdError:
            raise
        except (TypeError, ValueError) as exc:
            raise DomainIdError(suggested_id=PipelineId.suggest(value)) from exc

    def __str__(self) -> str:
        return self.value


__all__ = ["DomainId", "DomainIdError"]
