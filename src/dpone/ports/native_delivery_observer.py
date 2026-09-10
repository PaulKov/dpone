"""Optional diagnostic sink, independent of journal and business acceptance."""

from typing import Protocol

from dpone.contracts.native_delivery_observations import NativeDeliveryObservation


class NativeDeliveryObserver(Protocol):
    """Accept a bounded phase observation; callers isolate observer failures."""

    def record(self, observation: NativeDeliveryObservation) -> None:
        """Record diagnostic work only; never advance state or authorize cleanup."""
        ...
