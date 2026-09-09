from __future__ import annotations


def render_manifest_sparse_paths_text(view: object) -> str:
    return "".join(f"{entry.path}\n" for entry in view.report.entries)


__all__ = ["render_manifest_sparse_paths_text"]
