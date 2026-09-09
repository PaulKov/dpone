# FastTrack rollout runbook

This page describes the rollout pattern for the FastTrack connector.

## Canonical manifest

Use [examples/batch/landing_fasttrack_api.batch.yaml](https://github.com/PaulKov/dpone/blob/master/examples/batch/landing_fasttrack_api.batch.yaml) as the canonical manifest.

## Recommended tables

- `landing__fasttrack__chat_sessions`
- `landing__fasttrack__cascade_transactions`
- `landing__fasttrack__flex_cms_ratings`

## Schedule guidance

Choose a schedule that avoids the vendor maintenance window and leaves enough time for downstream transformations. Document the final SLA with the owning team.

## Required runtime conditions

- Writable package overlay directory only when using development install mode.
- Network access from the runtime to the package index, Vault, vendor API, and target sink.
- Consistent `dpone` package version across scheduler, worker, and trigger processes.

## Development rollout

1. Build or publish a snapshot package.
2. Configure development values.
3. Apply the rollout.
4. Run connector smoke.
5. Run manifest smoke for a small selector such as `flex_cms_ratings`.
6. Validate landing rows.

## Production rollout

1. Publish a normal release.
2. Deploy the baked package or image.
3. Apply production values.
4. Run connector smoke and one manifest selector.
5. Enable the schedule after validation.

## Checklist

- [ ] FastTrack source secret exists.
- [ ] Sink secret exists.
- [ ] Canonical manifest is available.
- [ ] Runtime starts with the expected `dpone` version.
- [ ] Connector smoke passed.
- [ ] Manifest smoke passed.
- [ ] Landing rows arrived.
- [ ] First scheduled run completed successfully.

## Related docs

- [FastTrack connector](fasttrack.md)
- [Development install mode](dev-install-mode.md)
- [Integration tests](testing/integration-tests.md)
