"""Small compatibility layer for optional google-cloud-bigquery objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class BigQueryJobConfigFactory:
    """Creates query job config objects with or without the optional BigQuery SDK."""

    def __init__(self) -> None:
        self._sdk = self._load_sdk()

    def query_job_config(self, *, destination: str, partition_value: str) -> Any:
        if self._sdk is None:
            return _QueryJobConfig(
                destination=destination,
                write_disposition=_WriteDisposition.WRITE_TRUNCATE,
                query_parameters=[_ScalarQueryParameter("partition_value", "STRING", partition_value)],
            )
        return self._sdk.QueryJobConfig(
            destination=destination,
            write_disposition=self._sdk.WriteDisposition.WRITE_TRUNCATE,
            query_parameters=[
                self._sdk.ScalarQueryParameter("partition_value", "STRING", partition_value),
            ],
        )

    @staticmethod
    def _load_sdk() -> Any | None:
        try:
            from google.cloud import bigquery
        except ModuleNotFoundError:
            return None
        return bigquery


@dataclass(frozen=True)
class _ScalarQueryParameter:
    name: str
    type_: str
    value: str


@dataclass(frozen=True)
class _QueryJobConfig:
    destination: str
    write_disposition: str
    query_parameters: list[_ScalarQueryParameter]


class _WriteDisposition:
    WRITE_TRUNCATE = "WRITE_TRUNCATE"
