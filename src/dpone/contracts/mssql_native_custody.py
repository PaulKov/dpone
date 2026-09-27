"""Closed, persistent target custody that is independent of lease expiry."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

RELEASE_REASONS = frozenset({"published_cleanup", "nonpublication_all_stages_retired", "empty_completion_cleanup"})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class NativeTargetCustodyRecord:
    """One target's closed CAS projection; a clear record remains durable."""

    schema_version: int
    kind: str
    target_id_sha256: str
    epoch: int
    state: str
    holder_invocation_key: str | None
    holder_lease_fence: int | None
    release_reason: str | None

    def __post_init__(self) -> None:
        held = self.state == "held"
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or self.kind != "dpone.mssql-native-target-custody"
            or _SHA256.fullmatch(self.target_id_sha256) is None
            or type(self.epoch) is not int
            or self.epoch < 1
            or self.state not in {"held", "clear"}
            or (
                held
                and (
                    not isinstance(self.holder_invocation_key, str)
                    or _SHA256.fullmatch(self.holder_invocation_key) is None
                    or type(self.holder_lease_fence) is not int
                    or self.holder_lease_fence < 1
                    or self.release_reason is not None
                )
            )
            or (
                not held
                and (
                    self.holder_invocation_key is not None
                    or self.holder_lease_fence is not None
                    or self.release_reason not in RELEASE_REASONS
                )
            )
        ):
            raise ValueError("mssql_native.invalid_target_custody")

    def payload(self) -> str:
        """Encode canonical JSON for deterministic CAS state."""
        return canonical_json_bytes(asdict(self)).decode()

    @classmethod
    def decode(cls, payload: str) -> NativeTargetCustodyRecord:
        """Reject unknown fields, noncanonical types, and malformed holder state."""
        try:
            value = strict_json_object(payload)
            if set(value) != set(cls.__dataclass_fields__):
                raise ValueError
            return cls(**value)
        except (TypeError, ValueError, KeyError) as error:
            raise ValueError("mssql_native.invalid_target_custody") from error
