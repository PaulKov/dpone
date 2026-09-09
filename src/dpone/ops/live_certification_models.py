from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class LiveCertificationStep:
    """One executable step in a live certification plan."""

    name: str
    command: str
    artifacts: tuple[str, ...]
    required: bool = True

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["artifacts"] = list(self.artifacts)
        return payload
