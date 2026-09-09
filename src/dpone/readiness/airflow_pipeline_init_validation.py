"""Side-effect-free validation for self-service pipeline initialization."""

from __future__ import annotations

from pathlib import Path

from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.readiness.airflow_pipeline_init_fixes import pipeline_id_fix_command
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url


def parse_pipeline_id(
    pipeline_id: str,
    *,
    recipe: str | None,
    route: str | None = None,
    airflow: bool | None,
    authoring_mode: str,
    profile: str | None,
    answers: str | Path | None,
) -> PipelineId | SelfServiceResult:
    """Parse an ID before catalog resolution while preserving authoring choices."""

    try:
        return PipelineId.parse(pipeline_id)
    except PipelineIdError as exc:
        fix_command = pipeline_id_fix_command(
            suggested_id=exc.suggested_id,
            recipe=recipe,
            route=route,
            airflow=airflow,
            authoring_mode=authoring_mode,
            profile=profile,
            answers=answers,
        )
        return SelfServiceResult(
            passed=False,
            errors=(
                dpone_error(
                    "DPONE_PIPELINE_ID_INVALID",
                    str(exc),
                    stage="init_pipeline",
                    docs_url=error_docs_url("DPONE_PIPELINE_ID_INVALID"),
                    fixes=[
                        {
                            "id": "use_suggested_pipeline_id",
                            "safety": "safe",
                            "command": fix_command,
                        }
                    ],
                    extra={"suggested_id": exc.suggested_id},
                ),
            ),
            exit_code=2,
        )


def safe_pipeline_id(value: object) -> str:
    """Return a canonical rerun argument or the parser's safe suggestion."""

    try:
        return str(PipelineId.parse(str(value)))
    except PipelineIdError as exc:
        return exc.suggested_id


__all__ = ["parse_pipeline_id", "safe_pipeline_id"]
