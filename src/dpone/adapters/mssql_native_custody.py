"""WindowStore CAS adapter for invocation-owned MSSQL target custody."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256

from dpone.contracts.bounded_window import WindowContractError, WindowLease
from dpone.contracts.mssql_native_custody import RELEASE_REASONS, NativeTargetCustodyRecord
from dpone.ports.bounded_window import WindowStore


@dataclass(frozen=True, slots=True)
class NativeCustodyClaim:
    """A matching new lease may inspect but cannot authorize a new launch."""

    record: NativeTargetCustodyRecord
    recovery_only: bool


class NativeTargetCustody:
    """Keep one stable holder across all chunks, crashes, and lease replacements."""

    def __init__(self, store: WindowStore, target_id: str) -> None:
        if type(target_id) is not str or not target_id:
            raise ValueError("mssql_native.invalid_custody_target")
        self.store, self.target_id = store, target_id
        self.target_id_sha256 = sha256(target_id.encode()).hexdigest()
        self.key = "mssql-native-target-custody-v1/" + self.target_id_sha256

    def _read(self, lease: WindowLease) -> tuple[int | None, NativeTargetCustodyRecord | None]:
        if lease.target_id != self.target_id:
            raise WindowContractError("mssql_native.custody_target_changed")
        self.store.assert_lease(lease)
        stored = self.store.load(self.key)
        if stored is None:
            return None, None
        try:
            record = NativeTargetCustodyRecord.decode(stored.payload)
        except ValueError as error:
            raise WindowContractError("mssql_native.invalid_target_custody") from error
        if record.target_id_sha256 != self.target_id_sha256:
            raise WindowContractError("mssql_native.custody_target_changed")
        return stored.revision, record

    def inspect(self, lease: WindowLease) -> NativeTargetCustodyRecord | None:
        """Read current custody under an active target lease."""
        return self._read(lease)[1]

    def assert_available_for_v1(self, lease: WindowLease) -> None:
        """The released path may start only when no v2 holder remains."""
        record = self.inspect(lease)
        if record is not None and record.state == "held":
            raise WindowContractError("mssql_native.custody_held")

    def claim(self, lease: WindowLease, invocation_key: str) -> NativeCustodyClaim:
        """CAS-claim before source I/O; same holder on a new fence is recovery-only."""
        if (
            type(invocation_key) is not str
            or len(invocation_key) != 64
            or any(character not in "0123456789abcdef" for character in invocation_key)
        ):
            raise ValueError("mssql_native.invalid_invocation_key")
        revision, previous = self._read(lease)
        if previous is not None and previous.state == "held":
            if previous.holder_invocation_key != invocation_key:
                raise WindowContractError("mssql_native.custody_held")
            return NativeCustodyClaim(previous, previous.holder_lease_fence != lease.fence)
        next_record = NativeTargetCustodyRecord(
            1,
            "dpone.mssql-native-target-custody",
            self.target_id_sha256,
            1 if previous is None else previous.epoch + 1,
            "held",
            invocation_key,
            lease.fence,
            None,
        )
        self.store.save(self.key, revision, next_record.payload(), lease)
        return NativeCustodyClaim(next_record, False)

    def reassert_grant(self, lease: WindowLease, invocation_key: str) -> None:
        """Reject drift or a new lease before any new writer grant."""
        record = self.inspect(lease)
        if record is None or record.state != "held" or record.holder_invocation_key != invocation_key:
            raise WindowContractError("mssql_native.custody_held")
        if record.holder_lease_fence != lease.fence:
            raise WindowContractError("mssql_native.custody_recovery_only")

    def release(
        self,
        lease: WindowLease,
        invocation_key: str,
        reason: str,
        *,
        assert_release_authority: Callable[[], None],
    ) -> NativeTargetCustodyRecord:
        """Clear only after caller proves matching publication or complete retirement."""
        if reason not in RELEASE_REASONS:
            raise ValueError("mssql_native.invalid_custody_release")
        revision, previous = self._read(lease)
        if previous is None or previous.state != "held" or previous.holder_invocation_key != invocation_key:
            raise WindowContractError("mssql_native.custody_held")
        assert_release_authority()
        cleared = NativeTargetCustodyRecord(
            1,
            "dpone.mssql-native-target-custody",
            self.target_id_sha256,
            previous.epoch,
            "clear",
            None,
            None,
            reason,
        )
        self.store.save(self.key, revision, cleared.payload(), lease)
        return cleared
