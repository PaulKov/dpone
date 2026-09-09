"""Shared constants for local non-runnable Airflow preview materialization."""

from __future__ import annotations

import hashlib

PREVIEW_RUNTIME_IMAGE = "local/dpone-runtime:preview"
PREVIEW_RUNTIME_IMAGE_DIGEST = "sha256:" + hashlib.sha256(b"dpone-local-preview-runtime").hexdigest()

__all__ = ["PREVIEW_RUNTIME_IMAGE", "PREVIEW_RUNTIME_IMAGE_DIGEST"]
