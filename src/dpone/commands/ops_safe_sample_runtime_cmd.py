from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any


def cmd_safe_sample_runtime_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Run safe-sample runtime with command-layer public contract validation."""

    setattr(args, "execution_plan_validator", _execution_plan_schema_issues)
    service_module = import_module("dpone.services.ops.command_handlers_safe_sample")
    return service_module.cmd_safe_sample_runtime_run(args, ctx=ctx, logger=logger)


@dataclass(frozen=True, slots=True)
class _PlanValidationIssue:
    code: str
    message: str
    path: str
    source: str


def _execution_plan_schema_issues(payload: Mapping[str, Any]) -> tuple[_PlanValidationIssue, ...]:
    validator_module = import_module("dpone.gitops.schema_validation")

    return tuple(
        _PlanValidationIssue(
            code=issue.code,
            message=issue.message,
            path=issue.path,
            source=issue.source,
        )
        for issue in validator_module.GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.safe-sample-execution-plan.v1",
        )
    )


__all__ = ["cmd_safe_sample_runtime_run"]
