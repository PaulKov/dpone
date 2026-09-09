"""Generate the versioned dpone Studio OpenAPI contract."""

from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.studio_openapi import studio_openapi_document

OUTPUT_PATH = Path("docs/api/studio-openapi.json")


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(
            studio_openapi_document(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
