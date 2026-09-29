"""Closed operator registration proof; database readback still grants authority.

These immutable values reject incomplete inventories and ambiguous bootstrap
inputs offline. The registrar must repeat the predicates under the protected
inventory barrier and derive actor/time from its authenticated connection.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, fields

from dpone.contracts.airflow_desired_state_validation import canonical_uuid, digest, text, utc_timestamp
from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_workspace_channel import (
    WorkspaceChannel,
    WorkspaceHandoverError,
    decode_workspace_document,
    require_workspace_document,
    require_workspace_size,
)
from dpone.contracts.dbt_workspace_registration_baseline import WorkspaceAdoptedCurrent

INVENTORY_SCHEMA = "dpone.dbt-workspace-registration-inventory.v1"
INPUT_SCHEMA = "dpone.dbt-workspace-registration-input.v1"


@dataclass(frozen=True, slots=True)
class WorkspaceRegistrationInventoryItem:
    """One protected occurrence, including disjoint resources and retired history."""

    activation_id: str
    request_sha256: str
    state: str
    bound_channel_sha256: str | None

    def __post_init__(self) -> None:
        try:
            canonical_uuid(self.activation_id, field="activation_id")
            digest(self.request_sha256, field="request_sha256")
            if self.state not in {"PREPARED", "ACTIVE", "RETIRING", "RETIRED"}:
                raise ValueError
            if self.bound_channel_sha256 is not None:
                digest(self.bound_channel_sha256, field="bound_channel_sha256")
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("registration_inventory_item") from None


@dataclass(frozen=True, slots=True)
class WorkspaceRegistrationInventory:
    """Bounded complete snapshot, never a guard-overlap-filtered discovery result."""

    occurrences: tuple[WorkspaceRegistrationInventoryItem, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.occurrences, tuple)
            or len(self.occurrences) > 8192
            or any(not isinstance(item, WorkspaceRegistrationInventoryItem) for item in self.occurrences)
        ):
            raise WorkspaceHandoverError("registration_inventory")
        identities = tuple(item.activation_id for item in self.occurrences)
        if tuple(sorted(set(identities))) != identities:
            raise WorkspaceHandoverError("registration_inventory_order")
        for item in self.occurrences:
            item.__post_init__()

    def _body(self) -> dict[str, object]:
        return {"schema": INVENTORY_SCHEMA, "occurrences": [asdict(item) for item in self.occurrences]}

    @property
    def inventory_sha256(self) -> str:
        return canonical_fingerprint(self._body())

    def to_dict(self) -> dict[str, object]:
        return {**self._body(), "inventory_sha256": self.inventory_sha256}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceRegistrationInventory:
        data = require_workspace_document(
            value,
            schema=INVENTORY_SCHEMA,
            names={"occurrences"},
            digest_field="inventory_sha256",
            maximum=8 * 1024 * 1024,
        )
        raw = data["occurrences"]
        if not isinstance(raw, list) or len(raw) > 8192:
            raise WorkspaceHandoverError("registration_inventory")
        items = []
        for item in raw:
            if not isinstance(item, dict) or set(item) != {
                field.name for field in fields(WorkspaceRegistrationInventoryItem)
            }:
                raise WorkspaceHandoverError("registration_inventory_fields")
            items.append(WorkspaceRegistrationInventoryItem(**item))
        return cls(tuple(items))


@dataclass(frozen=True, slots=True)
class WorkspaceEmptyAttestation:
    """Explicit privileged assertion, including when the SQL inventory is empty."""

    assertion: str
    retired_predecessor_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.retired_predecessor_ids, tuple) or len(self.retired_predecessor_ids) > 8192:
                raise ValueError
            for identity in self.retired_predecessor_ids:
                canonical_uuid(identity, field="retired_predecessor_id")
            if tuple(sorted(set(self.retired_predecessor_ids))) != self.retired_predecessor_ids:
                raise ValueError
            if self.assertion == "channel_never_applied":
                if self.retired_predecessor_ids:
                    raise ValueError
            elif self.assertion != "all_channel_predecessors_retired" or not self.retired_predecessor_ids:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("empty_attestation") from None

    def to_dict(self) -> dict[str, object]:
        return {"assertion": self.assertion, "retired_predecessor_ids": list(self.retired_predecessor_ids)}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceEmptyAttestation:
        if not isinstance(value, dict) or set(value) != {"assertion", "retired_predecessor_ids"}:
            raise WorkspaceHandoverError("empty_attestation_fields")
        if not isinstance(value["retired_predecessor_ids"], list):
            raise WorkspaceHandoverError("empty_attestation_ids")
        return cls(value["assertion"], tuple(value["retired_predecessor_ids"]))


@dataclass(frozen=True, slots=True)
class WorkspaceRegistrationInput:
    """Replayable reviewed input; it does not itself authorize registration."""

    registration_id: str
    channel: WorkspaceChannel
    mode: str
    reason: str
    operator_reference: str
    inventory: WorkspaceRegistrationInventory
    empty_attestation: WorkspaceEmptyAttestation | None
    adopted_current: WorkspaceAdoptedCurrent | None

    def __post_init__(self) -> None:
        try:
            canonical_uuid(self.registration_id, field="registration_id")
            text(self.reason, field="reason", maximum=1024)
            text(self.operator_reference, field="operator_reference", maximum=256)
            if len(self.reason.encode("utf-8")) > 1024 or len(self.operator_reference.encode("utf-8")) > 256:
                raise ValueError
            if not isinstance(self.channel, WorkspaceChannel) or not isinstance(
                self.inventory, WorkspaceRegistrationInventory
            ):
                raise ValueError
            self.channel.__post_init__()
            self.inventory.__post_init__()
            if self.mode == "empty":
                self._require_empty()
            elif self.mode == "adopt_active":
                self._require_adoption()
            else:
                raise ValueError
            require_workspace_size(self.to_dict(), 31 * 1024 * 1024)
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("registration_input") from None

    def _require_empty(self) -> None:
        if not isinstance(self.empty_attestation, WorkspaceEmptyAttestation) or self.adopted_current is not None:
            raise ValueError
        self.empty_attestation.__post_init__()
        if any(item.bound_channel_sha256 is None and item.state != "RETIRED" for item in self.inventory.occurrences):
            raise ValueError
        retired = {
            item.activation_id
            for item in self.inventory.occurrences
            if item.state == "RETIRED" and item.bound_channel_sha256 is None
        }
        if not set(self.empty_attestation.retired_predecessor_ids) <= retired:
            raise ValueError

    def _require_adoption(self) -> None:
        if not isinstance(self.adopted_current, WorkspaceAdoptedCurrent) or self.empty_attestation is not None:
            raise ValueError
        self.adopted_current.__post_init__()
        self.adopted_current.require_channel(self.channel)
        expected = WorkspaceRegistrationInventoryItem(
            self.adopted_current.activation_id, self.adopted_current.request_sha256, "ACTIVE", None
        )
        if expected not in self.inventory.occurrences:
            raise ValueError

    def _body(self) -> dict[str, object]:
        return {
            "schema": INPUT_SCHEMA,
            "registration_id": self.registration_id,
            "channel": self.channel.to_dict(),
            "mode": self.mode,
            "reason": self.reason,
            "operator_reference": self.operator_reference,
            "inventory": self.inventory.to_dict(),
            "empty_attestation": None if self.empty_attestation is None else self.empty_attestation.to_dict(),
            "adopted_current": None if self.adopted_current is None else self.adopted_current.to_dict(),
        }

    @property
    def registration_input_sha256(self) -> str:
        return canonical_fingerprint(self._body())

    def to_dict(self) -> dict[str, object]:
        return {**self._body(), "registration_input_sha256": self.registration_input_sha256}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceRegistrationInput:
        data = require_workspace_document(
            value,
            schema=INPUT_SCHEMA,
            names={item.name for item in fields(cls)},
            digest_field="registration_input_sha256",
            maximum=31 * 1024 * 1024,
        )
        data["channel"] = WorkspaceChannel.from_mapping(data["channel"])
        data["inventory"] = WorkspaceRegistrationInventory.from_mapping(data["inventory"])
        if data["empty_attestation"] is not None:
            data["empty_attestation"] = WorkspaceEmptyAttestation.from_mapping(data["empty_attestation"])
        if data["adopted_current"] is not None:
            data["adopted_current"] = WorkspaceAdoptedCurrent.from_mapping(data["adopted_current"])
        return cls(**data)

    @classmethod
    def from_json(cls, value: bytes | str) -> WorkspaceRegistrationInput:
        return cls.from_mapping(decode_workspace_document(value, 31 * 1024 * 1024))


@dataclass(frozen=True, slots=True)
class WorkspaceRegistrationActor:
    """Database-derived identity; a constructed value does not confer authority."""

    original_login: str
    database_user: str
    database_principal_id: int

    def __post_init__(self) -> None:
        try:
            text(self.original_login, field="original_login", maximum=512)
            text(self.database_user, field="database_user", maximum=512)
            if type(self.database_principal_id) is not int or not 1 <= self.database_principal_id <= 2**31 - 1:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("registration_actor") from None


@dataclass(frozen=True, slots=True)
class WorkspaceRegistrationReceipt:
    """Full immutable database receipt, including pinned input and derived actor."""

    input: WorkspaceRegistrationInput
    database_actor: WorkspaceRegistrationActor
    registered_at_utc: str

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.input, WorkspaceRegistrationInput) or not isinstance(
                self.database_actor, WorkspaceRegistrationActor
            ):
                raise ValueError
            self.input.__post_init__()
            self.database_actor.__post_init__()
            utc_timestamp(self.registered_at_utc, field="registered_at_utc")
            if not re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z", self.registered_at_utc
            ):
                raise ValueError
            require_workspace_size(self.to_dict(), 32 * 1024 * 1024)
        except (ValueError, TypeError, AttributeError):
            raise WorkspaceHandoverError("registration_receipt") from None

    def _body(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-workspace-registration-receipt.v1",
            "input": self.input.to_dict(),
            "database_actor": asdict(self.database_actor),
            "registered_at_utc": self.registered_at_utc,
        }

    @property
    def registration_sha256(self) -> str:
        return canonical_fingerprint(self._body())

    def to_dict(self) -> dict[str, object]:
        return {**self._body(), "registration_sha256": self.registration_sha256}

    @classmethod
    def from_mapping(cls, value: object) -> WorkspaceRegistrationReceipt:
        data = require_workspace_document(
            value,
            schema="dpone.dbt-workspace-registration-receipt.v1",
            names={item.name for item in fields(cls)},
            digest_field="registration_sha256",
            maximum=32 * 1024 * 1024,
        )
        actor = data["database_actor"]
        if not isinstance(actor, dict) or set(actor) != {item.name for item in fields(WorkspaceRegistrationActor)}:
            raise WorkspaceHandoverError("registration_actor_fields")
        data["database_actor"] = WorkspaceRegistrationActor(**actor)
        data["input"] = WorkspaceRegistrationInput.from_mapping(data["input"])
        return cls(**data)

    @classmethod
    def from_json(cls, value: bytes | str) -> WorkspaceRegistrationReceipt:
        return cls.from_mapping(decode_workspace_document(value, 32 * 1024 * 1024))
