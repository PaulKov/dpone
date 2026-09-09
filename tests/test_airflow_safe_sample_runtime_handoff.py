from __future__ import annotations

import json
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.services.safe_sample_runtime_handoff_plan import (
    SafeSampleRuntimeHandoffPathError,
    write_safe_sample_runtime_handoff,
)


@dataclass(frozen=True)
class _Plan:
    environment: str = "development"
    deployment_context: object = field(default_factory=lambda: SimpleNamespace(deployment_id="sha256:" + "d" * 64))
    temporary_target_plan: object = field(default_factory=lambda: SimpleNamespace(pipeline_id="orders_daily"))
    source_snapshot: object = field(
        default_factory=lambda: SimpleNamespace(
            to_dict=lambda: {
                "pipeline_id": "orders_daily",
                "path": "pipelines/orders_daily/pipeline.yaml",
                "sha256": "sha256:" + "f" * 64,
            }
        )
    )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.safe-sample-execution-plan.v1",
            "deployment_context": {"deployment_id": getattr(self.deployment_context, "deployment_id")},
            "source_snapshot": self.source_snapshot.to_dict(),
        }


def test_runtime_handoff_includes_complete_pinned_live_overlay_inputs(tmp_path: Path) -> None:
    result = write_safe_sample_runtime_handoff(
        _Plan(),  # type: ignore[arg-type]
        output_dir=tmp_path / "run",
        pipeline_source_path="pipelines/orders_daily/pipeline.yaml",
    )

    command = shlex.split(result["live_copy_command"])
    authorization_root = ".dpone-cache/route-authorizations/" + "sha256-" + "d" * 64 + "/orders_daily"
    expected = {
        "--route-attestation": f"{authorization_root}/route-attestation.json",
        "--route-attestation-bundle": f"{authorization_root}/route-attestation.sigstore.json",
        "--route-certification-bundle": f"{authorization_root}/route-certification-bundle.json",
        "--route-attestation-policy": f"{authorization_root}/route-attestation-policy.json",
    }
    for option, value in expected.items():
        index = command.index(option)
        assert command[index + 1] == value
        assert value in result["live_copy_requires"]
    assert "current" not in result["live_copy_command"]
    persisted = (tmp_path / "run/safe-sample-execution-plan.json").read_text(encoding="utf-8")
    assert '"source_snapshot"' in persisted


def test_runtime_handoff_v1_keeps_snapshot_inside_hash_bound_plan() -> None:
    schema = json.loads(Path("docs/schemas/gitops/safe-sample-runtime-handoff.schema.json").read_text(encoding="utf-8"))

    assert schema["additionalProperties"] is False
    assert "source_snapshot" not in schema["properties"]


def test_runtime_handoff_rejects_legacy_plan_without_source_snapshot(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"

    with pytest.raises(SafeSampleRuntimeHandoffPathError) as exc:
        write_safe_sample_runtime_handoff(
            _Plan(source_snapshot=None),  # type: ignore[arg-type]
            output_dir=output_dir,
            pipeline_source_path="pipelines/orders_daily/pipeline.yaml",
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING"
    assert not output_dir.exists()


@pytest.mark.parametrize("pipeline_id", ["../orders", "current"])
def test_runtime_handoff_rejects_unsafe_live_overlay_identity(tmp_path: Path, pipeline_id: str) -> None:
    plan = _Plan(temporary_target_plan=SimpleNamespace(pipeline_id=pipeline_id))
    output_dir = tmp_path / "run"

    with pytest.raises(SafeSampleRuntimeHandoffPathError) as exc:
        write_safe_sample_runtime_handoff(
            plan,  # type: ignore[arg-type]
            output_dir=output_dir,
            pipeline_source_path="pipelines/orders_daily/pipeline.yaml",
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"
    assert not output_dir.exists()


def test_runtime_handoff_does_not_publish_absolute_paths_outside_project(tmp_path: Path) -> None:
    result = write_safe_sample_runtime_handoff(
        _Plan(),  # type: ignore[arg-type]
        output_dir=tmp_path / "run",
        pipeline_source_path=tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml",
    )

    assert result["plan_path"] == "$OUTPUT_ROOT/safe-sample-execution-plan.json"
    assert tmp_path.as_posix() not in str(result)
