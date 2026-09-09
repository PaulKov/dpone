"""Shared BigQuery runtime primitives."""

from __future__ import annotations


def _require_bigquery():
    try:
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in minimal envs
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for BigQuery runtime operations. Install the 'gcp' extra."
        ) from exc
    return bigquery


def format_gcs_uri_for_display(gcs_uri: str | list[str]) -> str:
    """
    Форматирует GCS URI для отображения в логах.

    Args:
        gcs_uri: Один URI или список URIs

    Returns:
        Строка для отображения в логах
    """
    if isinstance(gcs_uri, list):
        return f"[{len(gcs_uri)} URIs]"
    return gcs_uri


__all__ = ["_require_bigquery", "format_gcs_uri_for_display"]
