# Unified run evidence pack

`dpone` can collect runtime artifacts into a single checksumed evidence index.
This is useful for certification, release review, incident response, and
catalog ingestion.

## Artifact shape

```text
.dpone/runs/<run_id>/
  run.json
  data_contract_evidence.json
  physical_ddl.json
  compatibility.json
  quality.json
  reconciliation.json
  openlineage_event.json
  unified_run_evidence.json
  unified_run_evidence.md
```

## Python API

```python
from dpone.ops.evidence_pack import UnifiedRunEvidencePackWriter

artifact = UnifiedRunEvidencePackWriter(".dpone/runs/01J...").write(
    run_id="01J...",
    artifacts={
        "run": ".dpone/runs/01J.../run.json",
        "data_contract": ".dpone/runs/01J.../data_contract_evidence.json",
        "physical_ddl": ".dpone/runs/01J.../physical_ddl.json",
    },
)
```

The JSON pack stores:

| Field | Meaning |
| --- | --- |
| `schema_version` | `dpone.unified_run_evidence.v1` |
| `run_id` | Stable run id. |
| `passed` | False when any attached artifact is missing or explicitly failed. |
| `items[*].sha256` | Checksum for immutable audit comparison. |

## Runbook

| Symptom | Action |
| --- | --- |
| `passed=false` | Open `items` and regenerate the failed/missing evidence. |
| checksum drift | Treat as a new evidence version and re-run release gate. |
| artifact not JSON | The pack still records checksum, but cannot infer `artifact_passed`. |

Related docs:

- [`dpone ops`](ops-cli.md)
- [OpenLineage and run history](lineage.md)
- [Certification suite](certification-suite.md)
