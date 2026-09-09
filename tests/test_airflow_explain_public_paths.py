from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.readiness.airflow_authoring_check_service import CheckedPipelineSource
from dpone.readiness.airflow_explain_service import AirflowExplainService
from dpone.readiness.airflow_self_service_models import SelfServiceResult


class _AuthoringCheck:
    def __init__(self, checked: CheckedPipelineSource) -> None:
        self._checked = checked

    def inspect(self, _target: str) -> CheckedPipelineSource:
        return self._checked


def test_explain_normalizes_absolute_input_to_project_relative_public_paths(tmp_path: Path) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source.parent.mkdir(parents=True)
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    checked = CheckedPipelineSource(
        source_path=source,
        payload={"metadata": {"id": "orders_daily"}},
        compilation=SimpleNamespace(semantic_fingerprint="sha256:" + "a" * 64),
        result=SelfServiceResult(passed=True),
        source_label="pipelines/orders_daily/pipeline.yaml",
    )

    result = AirflowExplainService(
        root=tmp_path,
        authoring_check=_AuthoringCheck(checked),  # type: ignore[arg-type]
    ).explain(source.as_posix())

    assert result.details is not None
    assert result.details["pipeline_ref"] == "pipelines/orders_daily/pipeline.yaml"
    assert result.details["source_path"] == "pipelines/orders_daily/pipeline.yaml"
    assert tmp_path.as_posix() not in str(result.to_dict())
