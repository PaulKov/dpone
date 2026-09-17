"""Finite launch context; the existing runner remains the sole process owner."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from threading import Event
from types import MappingProxyType
from typing import Protocol

from dpone.contracts.dbt_physical_transport_delivery import DELIVERY_ENVIRONMENT_KEY


@dataclass(frozen=True, slots=True)
class PhysicalTransportLaunchContext:
    """Only one inherited descriptor, one reserved env addition and owner bounds.

    The caller must use close_fds=True and consume this within the owning lease.
    Neither this DTO nor descriptor numbers constitute admission authority.
    """

    pass_fds: tuple[int]
    deadline_monotonic_ns: int
    cancellation: Event = field(repr=False, compare=False)
    environment: Mapping[str, str] = field(init=False)

    def __post_init__(self) -> None:
        if type(self.pass_fds) is not tuple or len(self.pass_fds) != 1:
            raise ValueError("physical launch requires exactly one descriptor")
        fd = self.pass_fds[0]
        if type(fd) is not int or fd < 3:
            raise ValueError("physical launch descriptor must not replace standard streams")
        if type(self.deadline_monotonic_ns) is not int or self.deadline_monotonic_ns <= 0:
            raise ValueError("physical launch requires an absolute deadline")
        if type(self.cancellation) is not Event:
            raise ValueError("physical launch requires its owner cancellation event")
        object.__setattr__(self, "environment", MappingProxyType({DELIVERY_ENVIRONMENT_KEY: str(fd)}))


class PhysicalTransportLaunch(Protocol):
    """One-use spawn-only lease; never owns Popen, wait, retries or settlement."""

    def __enter__(self) -> PhysicalTransportLaunchContext: ...

    def __exit__(self, *exc: object) -> None: ...
