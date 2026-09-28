"""Shared runtime boundary for the durable replay store and wire contracts.

Runtime governance and sink implementations consume one capability surface;
canonical values remain owned by contracts and the store interface by ports.
No authority, policy or serialization decisions are made by this module.
"""

from dpone.ports.quality_replay import QualityReplayStore, canonical_json_bytes, contracts

__all__ = ["QualityReplayStore", "canonical_json_bytes", "contracts"]
