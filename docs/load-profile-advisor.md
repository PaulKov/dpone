# Load Profile Advisor

`dpone profile advise` recommends the fastest safe load profile for a concrete
source-to-target route. It is a self-service layer above route capabilities,
runtime decision audit and certification evidence: instead of tuning chunk size,
route and fallback policy manually, engineers get an explainable recommendation
and a manifest patch.

## Commands

```bash
dpone profile advise path/to/manifest.yaml \
  --worker-profile weak_worker \
  --goal balanced \
  --row-count 33477752 \
  --column-count 12 \
  --estimated-bytes-per-row 24 \
  --format md
```

```bash
dpone profile wizard path/to/manifest.yaml \
  --worker-profile throughput \
  --goal performance \
  --estimated-bytes-per-row 24 \
  --output profile.patch.yaml \
  --format json
```

`advise` is read-only and CI-friendly. `wizard` writes a YAML patch that can be
reviewed in Git before changing the workload manifest.

`--goal performance` asks the advisor to optimize for throughput. It still
keeps worker memory guardrails and does not silently switch routes, but it makes
the recommendation explicit in evidence, prefers the supplied throughput worker
profile, and adds a warning to certify MSSQL source impact before making the
profile the default. Use `balanced` for routine conservative tuning.

## What The Advisor Checks

The first production profile is columnar snapshot delivery, but the taxonomy is
route-neutral:

- source shape: rows, columns, estimated bytes per row, source kind;
- sink route: object-storage pull, direct columnar push, typed binary stream,
  typed raw stream or native file fallback;
- worker limits: weak, balanced or throughput profile;
- execution policy: `chunked`, `file` or `streaming`;
- route safety: lineage, DQ, cleanup, fallback observability.

For narrow tables the advisor detects when `target_chunk_bytes` is effectively
blocked by `max_chunk_rows`. For example, a `512MiB` target window may still be
cut into many small windows if the row cap stays at `1_000_000`. The patch raises
the row cap within worker memory guardrails instead of relying on manual tuning.
The output also includes `window_count_estimate`, so users can see the expected
cycle reduction before they run the workload. For a narrow `dim_history`-style
table, a throughput profile can reduce roughly `35` one-million-row windows to
`2` larger windows while keeping the same byte budget.

Runtime evidence for chunked object-storage pull now records stage microsteps:

- `source_read`;
- `parquet_write`;
- `object_upload`;
- `clickhouse_pull`;
- `window_cleanup`.

These microsteps are stored in staged load metadata and are safe to surface in
XCom/acceptance reports. If ClickHouse pull is fast but source/window production
is slow, the bottleneck is visible without manually joining logs and
`system.query_log`.

## Output Contract

JSON output uses `dpone.profile.advice.v1`:

```json
{
  "schema_version": "dpone.profile.advice.v1",
  "selected_profile": "object_storage_pull.chunked.weak_worker",
  "selected_route": "object_storage_pull",
  "execution_mode": "chunked",
  "expected_rps_range": {"min": 150000, "max": 900000},
  "warnings": ["row_cap_limits_target_chunk_bytes"],
  "details": {
    "optimization_goal": "balanced",
    "window_count_estimate": {"current": 34, "recommended": 7, "reduction": 27}
  },
  "recommended_patch": {
    "source": {
      "options": {
        "native_transfer": {
          "snapshot": {
            "columnar_fast_path": {
              "execution": {
                "mode": "chunked",
                "target_chunk_bytes": 536870912,
                "max_chunk_rows": 5000000,
                "max_inflight_chunks": 1,
                "cleanup_policy": "eager"
              }
            }
          }
        }
      }
    }
  }
}
```

The patch is intentionally manifest-shaped, so it can be applied by existing
GitOps review flows without introducing another configuration dialect.

## Product Pattern

- ClickHouse recommends large inserts and efficient formats; dpone maps that
  into route profiles and chunk sizing instead of leaving users to infer settings
  from server behavior: [ClickHouse insert strategy](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy).
- dlt exposes load info and traces for recently loaded data; dpone keeps the
  trace idea and adds a manifest patch for the next run: [dlt running](https://dlthub.com/docs/running-in-production/running).
- Airbyte exposes job history and logs for sync attempts; dpone persists route
  decisions and turns them into reusable profile advice: [Airbyte logs](https://docs.airbyte.com/platform/operator-guides/browsing-output-logs).
- Fivetran visualizes Extract, Process and Load timing in sync history; dpone
  uses the same phase discipline but keeps the recommendation OSS-visible:
  [Fivetran sync overview](https://fivetran.com/docs/core-concepts/syncoverview).
- Pentaho step metrics help identify the slowest transformation step; dpone
  automates the next tuning action through a typed profile recommendation:
  [Pentaho monitoring](https://docs.pentaho.com/pdia-admin/pdia-11.0-admin/pdi/transforming-data-with-pdi/logging-and-performance-monitoring/monitor-performance/monitoring-tab).

## Current Scope

The first implementation is deliberately small:

- it is read-only unless `wizard --output` is used;
- it accepts live shape facts from CLI flags or future probes;
- it generates patch recommendations for governed columnar chunking;
- it does not execute database probes itself.

Future versions can plug in acceptance artifacts, route certification results and
connector-specific shape probes behind the same advisor contracts.
