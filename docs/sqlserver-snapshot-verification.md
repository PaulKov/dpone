# SQL Server snapshot planning verification

Purpose: distinguish observations made during planning from future acceptance
and live certification. Audience: reviewers and maintainers. Return to the
[roadmap](sqlserver-snapshot-roadmap.md).

- Status: RESEARCHED; last verified: 2026-09-09.
- Source commit: `6533c27fbf77c78a00b8013bcf72e2293e281e1f`.
- Declared source version: 0.74.33 (`pyproject.toml:7`); accelerator source also
  declares 0.74.33. No matching local release tag was available. Exact published
  distribution equivalence is UNVERIFIED.
- Environment: isolated worktree, CPython 3.12.11, offline frozen dependency
  resolution and repository-local optional accelerator. No server was contacted.

## Executed checks

| Check | Status | Observed result / limits |
|---|---|---|
| Applicable instructions, feature template, targeted code/standards trace | PASS | Baseline fixed before edits; only planning documents written |
| Budget compiler/runtime synthetic mapping | PASS reproduction of defect | Compiled max_source_bytes retained; LoadConfig loses it |
| Public strategy sub-schema validation | PASS reproduction of defect | max_source_bytes rejected as additional property; no universal runtime barrier inferred |
| Five bounded-policy schema cases | PASS | Existing policy admission/rejection tests; not runtime enforcement |
| Native physical contracts/capability/reconciliation and dbt physical authority | PASS | 79 existing offline cases |
| Generic ClickHouse lifecycle/finalize/namespace and MSSQL staging lifecycle | PASS | 32 existing offline cases; no crash/transaction live proof |
| Synthetic catalog ARCHIVE observation | PASS reproduction of defect | Actual catalog string COLUMNSTORE_ARCHIVE becomes NONE |
| Independent literal UUID/decimal/numeric wire fixtures | PASS reproduction of defect | Wrong prefix causes wrong value and sentinel; in-memory corrected prefix restores exact consumption/value |
| Existing binary transport suite, first run | FAIL environment prerequisite | Optional dpone_native_accel distribution absent; one metadata test failed |
| Same suite after offline local accelerator installation | PASS | 77 cases; this does not invalidate the independent defect reproduction |
| Route docs command without manifest path | FAIL documented example | Missing required path, exit 2; no data movement |
| Current official primary sources | PASS read-only research | Version/date and source links recorded; no performance winner inferred |
| Live BCP exporter, MSSQL/ClickHouse route, failure/cancellation and resource benchmarks | SKIP / UNVERIFIED | No approved dedicated environment; offline fixtures are not exporter captures |
| Published 0.74.33 wheel/tag comparison | UNVERIFIED | Source declares that version, but exact release identity was not available locally |
| Production implementation, CI policy edits, commit/PR/release | N/A | Explicitly excluded |

## Reproducible command groups

Commands are repository-relative and contain no credentials. `--offline --frozen`
prevents dependency resolution from changing pins. The optional accelerator was
installed from this repository into the isolated environment with
`uv pip install --offline --no-deps -e packages/dpone-native-accel`.

```bash
uv run --offline --frozen pytest \
  tests/test_mssql_physical_design_contracts.py \
  tests/test_mssql_physical_design_capability_matrix.py \
  tests/test_mssql_physical_reconciliation_contracts.py \
  tests/test_dbt_workspace_mssql_physical_authority.py -q

uv run --offline --frozen pytest \
  tests/test_clickhouse_staging_lifecycle.py \
  tests/test_clickhouse_production_finalize.py \
  tests/test_clickhouse_managed_artifacts_schema.py \
  tests/test_mssql_staging_consumer_lifecycle.py -q

.venv/bin/python -m pytest \
  tests/test_native_bcp_decoder.py \
  tests/test_native_bcp_decoder_contracts.py \
  tests/test_native_bcp_decoder_native_format.py \
  tests/test_native_accel_provider.py \
  tests/test_native_acceleration_contracts.py \
  tests/test_runtime_mssql_clickhouse_native_transfer.py \
  tests/test_tools_mssql_clickhouse_bcp_native_type_certification.py -q
```

The BCP spec contains an independent synthetic reproduction. No generated
certification artifacts were edited or manufactured. This document is a planning
observation record, not a machine-issued release or certification receipt.

## Documentation and privacy gates

Documentation gate results are recorded after the complete planning set exists.
Privacy review covers only new documents: neutral synthetic identifiers, no
customer attribution, credentials, internal addresses, task IDs, deployment data
or local user paths. Public repository-relative references and public vendor
source URLs are allowed. No private-data denylist is acquired to perform this
review. Link/file checks and manual content review complement pattern checks;
pattern matching alone is not a confidentiality guarantee.

| Documentation / review check | Status | Observation |
|---|---|---|
| Change-aware selector | PASS | Selected documentation checks and potential R6 scope |
| dpone docs check-docs | PASS | 801 Markdown files, 3161 local links at first complete-set run |
| dpone docs check-generated-references | PASS | 3/3 synchronized; no generated references changed |
| tests/test_docs_language_contracts.py | PASS | 32 cases |
| mkdocs build --strict | PASS | Strict build completed |
| Fresh-context review | PASS after revisions | Added missing reproducible snippets and closed ambiguous chunk-delivery retry semantics |
| Planning scope, links and privacy review | PASS | Only six English planning documents changed; neutral identifiers and public sources |

Ready for planning review only. Implementation, merge and release readiness are
not asserted. Maintainer approval and an isolated live environment remain future
requirements.


## Additional synthetic reproductions

Run the following from the repository environment. It constructs no source data
from external systems and writes no fixture files. Assertions document the
baseline defect, not the desired future behavior.

```python
import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

from jsonschema import Draft7Validator
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.services.dbt_publish_model_compiler import _runtime_strategy
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalIntrospector

strategy = _runtime_strategy({"mode": "full_refresh", "max_source_bytes": 1})
assert strategy["max_source_bytes"] == 1
config = {
    "source": {"type": "mssql", "connection_id": "source_ref",
               "table": {"schema": "sample", "name": "events"}},
    "sink": {"type": "clickhouse", "connection_id": "sink_ref",
             "table": {"schema": "sample", "name": "events"},
             "strategy": strategy},
}
runtime = asdict(LoadConfigBuilder().build(config))
assert "max_source_bytes" not in repr(runtime)
schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text())
strategy_schema = schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["strategy"]
errors = list(Draft7Validator(strategy_schema).iter_errors(strategy))
assert any(error.validator == "additionalProperties" for error in errors)

class SyntheticCatalog:
    def get_records(self, query):
        return [("COLUMNSTORE_ARCHIVE",)]

observed = MssqlPhysicalIntrospector(SyntheticCatalog())._compression(
    SimpleNamespace(target_database=None, target_schema="sample", target_table="events")
)
assert observed == "NONE"
print("PASS: reproduced runtime budget loss, schema mismatch and lossy catalog observation")
```

Exact policy-test selector used for the five existing cases:

```bash
.venv/bin/python -m pytest \
  tests/test_dbt_publish_schema_contracts.py::test_policy_schema_accepts_bounded_environment_neutral_strategy_policy \
  tests/test_dbt_publish_schema_contracts.py::test_policy_schema_rejects_unsafe_strategy_policy -q
```
