"""Production link between admitted v3 dispatch and the native dbt parent root.

These tests exercise the real Task 4 dispatch boundary, the real runtime dbt
bootstrap and the real supervised execution root over the same offline protected
doubles used by ``tests.test_composition_dbt_execution_root``. Only the Linux
supervisor, the dbt distribution and SQL Server are replaced. Live behaviour
remains UNVERIFIED here.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.composition_execution_authority import (
    COMPOSITION_SUPERVISOR_B64_ENV,
    RUNTIME_RELEASE_ADMISSION_ENV,
    supervisor_transport,
)
from dpone.contracts.release_composition import COMPOSITION_ADMISSION
from dpone.runtime.composition_native_dbt_dispatch import CompositionNativeDbtDispatcher
from dpone.runtime.composition_verified_dispatch import (
    CompositionDispatchRejection,
    CompositionRunVolume,
    composition_dispatch_request,
    run_composition_dispatch,
)
from tests.test_composition_dbt_execution_root import (
    SUPERVISOR,
    Harness,
    _scheduler_environment,
    _written_pack,
    build,
)


def _run_volume(tmp_path: Path) -> CompositionRunVolume:
    run = tmp_path / "run"
    run.mkdir(parents=True, exist_ok=True)
    volume = CompositionRunVolume(evidence_path=run / "runtime-evidence.json", stderr_path=run / "runtime-stderr.log")
    volume.stderr_path.touch()
    return volume


def _authority(transport: str | None = None) -> dict[str, str]:
    return {
        RUNTIME_RELEASE_ADMISSION_ENV: COMPOSITION_ADMISSION,
        COMPOSITION_SUPERVISOR_B64_ENV: transport or supervisor_transport(SUPERVISOR),
    }


def _request(harness: Harness, volume: CompositionRunVolume, **overrides: Any):
    authority = overrides.pop("authority", None) or _authority()
    environment = {**_scheduler_environment(harness), **authority}
    argv = overrides.pop(
        "argv",
        ("dpone", "dbt", "execute-pack", _written_pack(harness), "--format", "json"),
    )
    return composition_dispatch_request(
        argv=argv,
        working_directory=Path(harness.request.runtime_root),
        authority=authority,
        environment=environment,
        run_volume=volume,
    )


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    worker = tmp_path / "worker"
    worker.mkdir(parents=True, exist_ok=True)
    return build(worker)


def test_admitted_native_dbt_dispatch_reaches_the_supervised_root(harness: Harness, tmp_path: Path) -> None:
    """The verified argv, not a caller flag, selects the parent worker path."""
    volume = _run_volume(tmp_path)
    dispatcher = CompositionNativeDbtDispatcher(harness.root, supervisor=SUPERVISOR)

    status = run_composition_dispatch(
        dispatcher,
        argv=("dpone", "dbt", "execute-pack", _written_pack(harness), "--format", "json"),
        working_directory=Path(harness.request.runtime_root),
        authority=_authority(),
        environment={**_scheduler_environment(harness), **_authority()},
        run_volume=volume,
    )

    assert status == 0
    assert harness.events[-1] == "finalize_attempt"
    assert harness.attempts.terminal[0][0] == "SUCCEEDED"
    evidence = json.loads(volume.evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "passed"
    assert evidence == json.loads(harness.evidence_path().read_text(encoding="utf-8"))


def test_ordinary_transfer_stays_explicitly_unavailable(harness: Harness, tmp_path: Path) -> None:
    """Tasks 6-7 own the ordinary cells; v3 must not degrade to a generic child."""
    volume = _run_volume(tmp_path)
    request = _request(
        harness,
        volume,
        argv=("dpone", "run", "manifests/pricing.yml", "--format", "json"),
    )

    with pytest.raises(CompositionDispatchRejection, match="composition_ordinary_worker_unavailable") as exc_info:
        CompositionNativeDbtDispatcher(harness.root, supervisor=SUPERVISOR).run(request)

    assert exc_info.value.dispatch_started is False
    assert harness.events == []
    assert not volume.evidence_path.exists()


def test_dispatch_requires_the_pinned_supervisor_capability(harness: Harness, tmp_path: Path) -> None:
    other = supervisor_transport(replace(SUPERVISOR, persistent_volume_claim="other-claim"))
    request = _request(harness, _run_volume(tmp_path), authority=_authority(other))

    with pytest.raises(CompositionDispatchRejection, match="composition_supervisor_authority"):
        CompositionNativeDbtDispatcher(harness.root, supervisor=SUPERVISOR).run(request)

    assert harness.events == []


def test_evidence_that_disagrees_with_the_worker_is_refused(harness: Harness, tmp_path: Path) -> None:
    """A passing status can never be published against non-passing evidence."""
    volume = _run_volume(tmp_path)
    request = _request(harness, volume)

    def execute_pack(*_args: Any, **_kwargs: Any) -> Any:
        class _Evidence:
            status = "failed"

            @staticmethod
            def to_dict() -> dict[str, object]:
                return {"status": "failed"}

        class _Outcome:
            exit_code = 0
            evidence = _Evidence()
            passed = False

        return _Outcome()

    dispatcher = CompositionNativeDbtDispatcher(harness.root, supervisor=SUPERVISOR, execute_pack=execute_pack)
    with pytest.raises(CompositionDispatchRejection, match="composition_evidence_disagreement") as exc_info:
        dispatcher.run(request)

    assert exc_info.value.dispatch_started is True


def test_unavailable_composed_root_never_degrades_to_a_generic_child(harness: Harness, tmp_path: Path) -> None:
    volume = _run_volume(tmp_path)
    request = _request(harness, volume)

    with pytest.raises(CompositionDispatchRejection, match="composition_native_worker_unavailable"):
        CompositionNativeDbtDispatcher(None, supervisor=SUPERVISOR).run(request)  # type: ignore[arg-type]

    assert harness.events == []
