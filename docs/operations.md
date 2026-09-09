# Operations guide

This guide describes how to operate `dpone` in local, CI, staging, and
production-like environments. Operational controls are exposed through
[`dpone ops`](ops-cli.md); there is no separate `production` API or runtime
mode.

## Release channels

- GitHub release artifacts are attached to version tags.
- PyPI releases are published under the `dpone` distribution.
- The ordinary release workflow publishes through PyPI Trusted Publishing and
  requires the `pypi` environment's GitHub OIDC identity.
- `release.yml` never reads `PYPI_API_TOKEN`; the presence of a stored token
  cannot change an ordinary tag release.
- There is no token-based emergency publisher; never add a token to
  `release.yml` or the controller.

## Recommended deployment model

1. Pin `dpone` versions in production environments.
2. Promote releases through dev, staging, and production.
3. Keep provider credentials outside manifests.
4. Use environment variables, Airflow connections, Vault-compatible providers,
   or secret managers.
5. Enable structured log collection for every DAG run.

## Runtime health checks

For each production pipeline, track:

- Last successful run timestamp.
- Extracted row count.
- Loaded row count.
- Final row count.
- Error count.
- Retry count.
- Runtime duration.
- Checkpoint age.
- Soft-delete count when reconciliation is enabled.

## `dpone ops` toolbox

Use [`dpone ops`](ops-cli.md) when you need operational evidence or safe
incident controls:

```bash
dpone ops certification-run --source postgres --sink mssql --strategy snapshot_diff
dpone ops certification-pack --pack-id orders-release --artifact certification_report=test_artifacts/integration_matrix/certification_report.json
dpone ops recovery-plan --state-dir .dpone/orchestration-state --lock-dir .dpone/locks
dpone ops reconcile --source-rows-json "$SOURCE_SAMPLE" --target-rows-json "$TARGET_SAMPLE" --key id
dpone ops deploy-render --target k8s-cronjob --manifest manifests/orders.yml --selector daily_orders
dpone ops contract-check --rows-json '[{"id":1}]' --contract-json '{"required_columns":["id"]}'
dpone ops marketplace --format md
dpone ops rollback-plan --sink mssql --target landing.orders --load-id 01JLOAD0000000000000000000
```

The toolbox covers certification artifacts, data contracts, quarantine replay,
load package lifecycle, rollback plans, connector marketplace badges,
reconciliation, recovery planning, deployment handoff, object-storage staging
evidence, catalog publication, and links to performance advice. See the
[Operational control plane](operational-control-plane.md) guide for the full
operator flow and runbooks.

## Incident response

When a run fails:

1. Capture the manifest path and selector.
2. Capture the run ID and task ID.
3. Classify the failure as configuration, credentials, source, sink, schema,
   network, quota, or framework.
4. Check whether a checkpoint was written before failure.
5. Decide whether rerun is safe based on the connector's idempotency contract.
6. Record the failure mode in connector docs if it is new.

## Upgrade policy

- Patch upgrades should be safe for existing manifests.
- Minor upgrades may change behavior during `0.x`, but must document migration
  steps in `CHANGELOG.md`.
- Breaking changes must include a compatibility note and migration example.

## Secrets

Never commit or paste credentials into manifests, docs, issues, pull requests,
or logs. If a credential is exposed:

1. Revoke it immediately.
2. Rotate dependent systems.
3. Run repository and history secret scans.
4. Document the incident impact.
