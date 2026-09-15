"""Compatibility import for the native project archive adapter.

Capture and reading are owned by ``dpone.adapters.native_project_documents``;
pure document generation and validation remain in the contracts package.
"""

from dpone.adapters.native_project_documents import NativeProjectDocuments

__all__ = ["NativeProjectDocuments"]
