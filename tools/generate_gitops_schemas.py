"""Generate committed GitOps JSON Schema references from canonical producers."""

from __future__ import annotations

import json
from pathlib import Path

from dpone.gitops.schema_contracts import gitops_schema_contracts


def main() -> None:
    """Update only schema files whose semantic content is stale."""

    target = Path("docs/schemas/gitops")
    target.mkdir(parents=True, exist_ok=True)
    for contract in gitops_schema_contracts():
        path = target / f"{contract.name}.schema.json"
        rendered = (
            json.dumps(
                contract.schema,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        current = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
        if current == contract.schema:
            continue
        path.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
