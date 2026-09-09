"""Generate committed dbt self-service JSON Schema references."""

from __future__ import annotations

import json
from pathlib import Path

from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.contracts.semantic_refresh_schemas import semantic_refresh_contract_schemas


def main() -> None:
    target = Path("docs/schemas/dbt")
    target.mkdir(parents=True, exist_ok=True)
    contracts = {
        **dbt_schema_contracts(),
        **semantic_refresh_contract_schemas(),
    }
    for contract_id, schema in sorted(contracts.items()):
        path = target / f"{contract_id}.schema.json"
        path.write_text(json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
