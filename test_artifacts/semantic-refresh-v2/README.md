# Semantic refresh V2 evidence

This directory contains generated local evidence and path-scoped agent task
contracts for the V2 implementation. Generated evidence must come from its
checked-in producer; do not edit JSON results by hand.

After provisioning the disposable SQL Server, ClickHouse, MinIO/KES,
k3d/Vault, Airflow 3.3 and Docker dependencies described in the
[platform guide](../../docs/dbt-semantic-refresh-v2-platform.md#run-the-local-implementation-campaign),
run the full local Docker campaign from a clean repository root into a unique
ignored generation:

```bash
CAMPAIGN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
uv run python tools/semantic_refresh_local_campaign.py \
  --output ".dpone/generated/semantic-refresh-v2/${CAMPAIGN_ID}/local-docker-evidence.json"
```

The resulting report binds the current commit and source snapshot to the closed
test inventory. It stores only bounded statuses, durations, and redacted-output
digests. It intentionally reports production certification as `UNVERIFIED`:
local SQL Server, ClickHouse, MinIO/KES, Airflow, Kubernetes, and Vault evidence
cannot certify a consuming production deployment. Generated reports are not
checked in: retain them as access-controlled CI/release artifacts and never
copy an older PASS report into a new candidate.
