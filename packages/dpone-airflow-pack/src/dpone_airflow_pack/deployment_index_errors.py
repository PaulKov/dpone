"""Structured errors shared by deployment-index contracts."""

from __future__ import annotations


class AirflowDeploymentIndexError(RuntimeError):
    """Structured loader error for deployment index failures."""

    def __init__(self, code: str, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.path = path

    def to_jsonable(self) -> dict[str, str]:
        payload = {
            "schema": "dpone.error.v1",
            "code": self.code,
            "stage": "airflow_parse",
            "severity": "error",
            "message": str(self),
        }
        if self.path:
            payload["path"] = self.path
        return payload


__all__ = ["AirflowDeploymentIndexError"]
