# DPONE_LIVE_CHECK_PROBE_FAILED

At least one configured bounded live preflight probe returned `status: failed`.

This means the live runner was configured and reachable, but a dependency check
did not pass. Examples include credential resolution failure, a denied bounded
source probe, or a sink readiness failure.

## Fix

Inspect the failing `connection_ref` and probe in the `dpone.live-preflight.v1`
report:

```bash
dpone check pipelines/orders_daily --live --environment dev --format json
```

Fix the platform binding, registry entry, credential runtime, or source/sink
permissions, then rerun the live check. Do not bypass the failed probe for
production routes.
