# AppsFlyer rollout runbook

This page describes a production rollout pattern for the AppsFlyer connector: canonical manifest, environment overlays, manual smoke checks, and release checklist.

## Canonical manifest

Use the batch manifest in [examples/batch/landing_appsflyer_api.batch.yaml](https://github.com/PaulKov/dpone/blob/master/examples/batch/landing_appsflyer_api.batch.yaml) as the source of truth.

The Variant C model keeps AppsFlyer resources in one manifest so app identifiers, window logic, target dataset, and table naming are managed consistently.

## Recommended tables

- `landing__appsflyer__installs_report`
- `landing__appsflyer__in_app_events_report`
- `landing__appsflyer__uninstall_events_report`
- `landing__appsflyer__daily_report`

## Required secrets

- AppsFlyer source secret, for example `api/appsflyer`.
- Target warehouse secret for the selected sink.

All secrets must be stored in a supported secret backend and must never be committed to the repository.

## Development rollout

1. Build or publish a snapshot package.
2. Configure development values or environment variables.
3. Restart the scheduler/worker/runtime process if using a long-running orchestrator.
4. Run connector smoke.
5. Run one manifest selector with a small window.
6. Validate row counts in the landing target.
7. Enable the schedule after smoke checks are green.

## Production rollout

1. Publish a normal `dpone` release.
2. Deploy the immutable package or image.
3. Run connector smoke in the production window.
4. Run one manifest selector.
5. Validate row counts, duplicate checks, and freshness.
6. Enable the schedule.

## Smoke commands

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live -k appsflyer
```

## Checklist

- [ ] Source secret exists in the selected environment.
- [ ] Sink secret exists in the selected environment.
- [ ] Canonical manifest is available in the checkout.
- [ ] Runtime package version is pinned.
- [ ] Connector smoke passed.
- [ ] Manifest smoke passed for at least one selector.
- [ ] Landing rows arrived.
- [ ] Duplicate and row-count checks are green.
- [ ] Schedule is enabled.
- [ ] First scheduled run completed successfully.

## Related docs

- [AppsFlyer connector](appsflyer.md)
- [Integration tests](testing/integration-tests.md)
- [Release process](release.md)
