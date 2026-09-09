# CI quality performance evidence

The `CI` workflow keeps the visible required contexts `Quality checks (3.11)`
and `Quality checks (3.12)`.  Each context is an aggregate: it passes only
after all eight deterministic, exact-head non-live test shards for its Python
version have passed and supplied one receipt each. Python 3.12 additionally
combines all eight coverage inputs and applies the existing coverage ratchet.

The planner is intentionally deterministic and does not use past timing data:

```bash
uv run python tools/ci_quality_shards.py \
  --nodeids-file test_artifacts/ci-quality/nodeids.txt \
  --shard-count 8 --output test_artifacts/ci-quality/shard-manifest.json
```

A receipt must name the candidate SHA, exact interpreter, full population
digest, selected membership and duration. Missing, duplicate, wrong-version or
wrong-head receipts are errors; a cache or a previous workflow run is never
evidence. The aggregate validates these conditions before it combines coverage.

## Hosted latency decision

The release claim requires three independent GitHub-hosted executions of one
unchanged SHA. Their `ci-quality-timing-v1` artifacts are reduced locally:

```bash
uv run python tools/ci_quality_timing.py \
  --samples test_artifacts/ci-quality/timing-samples.json \
  --output test_artifacts/ci-quality/performance-decision.json
```

The reducer accepts exactly three distinct run-attempt identities, one
lower-case forty-character SHA, and `PASS` samples only. With three samples the
conservative nearest-rank p95 is their maximum. The decision passes only below
720 seconds, a greater-than-50% reduction from the accepted 26m20s baseline.
Queue delay is reported separately and does not improve an execution-duration
claim. Until all three artifacts are present and validate, latency DoD is
`UNVERIFIED`, even when a PR check is green.

For recovery steps, see [CI quality failures](runbooks.md#ci-quality-failures).
