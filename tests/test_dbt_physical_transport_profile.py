"""Profile authority cannot be substituted with a byte callback or fake DTO."""

import pytest

from dpone.adapters.dbt_physical_transport_profile import PhysicalTransportProfile
from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer
from dpone.adapters.native_dbt_profile_lease import NativeDbtProfileLease


def test_requires_concrete_renderer_and_lease(tmp_path):
    lease = NativeDbtProfileLease(tmp_path, max_bytes=65536)
    with pytest.raises(ValueError):
        PhysicalTransportProfile(renderer=lambda: b"profile", lease=lease)


def test_materialize_without_actual_render_and_snapshot_without_held_file_reject(tmp_path):
    class Resolver:
        def resolve(self, reference):
            pytest.fail("unexpected credential lookup")

    with NativeDbtProfileLease(tmp_path, max_bytes=65536) as lease:
        profile = PhysicalTransportProfile(renderer=RuntimeDbtProfileRenderer(Resolver()), lease=lease)
        with pytest.raises(ValueError):
            with profile.materialize(b"forged"):
                pytest.fail("caller bytes accepted")
        with pytest.raises(ValueError):
            profile.snapshot(lease.profile_path)
