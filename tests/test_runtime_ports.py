from __future__ import annotations

import subprocess
import sys
import textwrap


def test_runtime_ports_bootstrap_without_heavy_optional_imports() -> None:
    """The lazy bootstrap must register runtime adapters without importing
    optional Google client packages eagerly.

    The check runs in a fresh interpreter so it stays deterministic regardless
    of pytest collection order or xdist worker reuse (other test modules
    legitimately import google.cloud at module level).
    """

    script = textwrap.dedent(
        """
        import sys

        from dpone.ports.process_runner import ensure_process_runner
        from dpone.ports.runtime_hydrator import ensure_runtime_hydrator

        assert ensure_runtime_hydrator() is not None
        assert ensure_process_runner() is not None
        assert "google.cloud" not in sys.modules, "bootstrap must stay lazy"
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
