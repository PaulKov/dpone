"""Authority required for table-wide atomic window publication.

Implementations must exclude every writer, including direct SQL, scheduled jobs,
TTL mutations, and other dpone processes. A local journal lease or configuration
boolean cannot implement this authority. The deployment composition root supplies
an actual lock/access-control authority and maintains it through the operation.
"""

from contextlib import AbstractContextManager
from typing import Protocol

from dpone.contracts.bounded_window import WindowLease


class ExclusiveWindowWriterGuard(Protocol):
    """Trusted infrastructure boundary covering ALL target and staging writers."""

    def validate(self, target_id: str, physical_target: str) -> None:
        """Prove exclusive authority for this physical target, or raise."""

    def assert_lease(self, lease: WindowLease) -> None:
        """Assert live authority and fencing epoch at each mutation boundary."""

    def fence_attempt(self, lease: WindowLease, operation_id: str) -> None:
        """Stop/join old writers before return; operation_id is the staging table/query ID."""

    def hold(self, lease: WindowLease) -> AbstractContextManager[None]:
        """Hold backend exclusion through request completion, including lost replies.

        Expiration must stop/join active server writers before a successor enters.
        assert_lease alone is insufficient to satisfy this lifetime contract.
        """
