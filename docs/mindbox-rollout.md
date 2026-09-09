# Mindbox rollout runbook

This page describes the rollout pattern for the Mindbox connector.

## Canonical manifest

Use [examples/batch/landing_mindbox_api.batch.yaml](https://github.com/PaulKov/dpone/blob/master/examples/batch/landing_mindbox_api.batch.yaml) as the canonical manifest.

## Recommended tables

- `landing__mindbox__actions`
- `landing__mindbox__orders`
- `landing__mindbox__customers`

## Schedule guidance

Mindbox exports can be heavy. Keep live checks manual or scheduled and coordinate the extraction window with the owning team.

## Development rollout

1. Build or publish a snapshot package.
2. Configure development values.
3. Run connector smoke.
4. Run manifest smoke for one small selector, such as `actions`.
5. Validate landing rows.
6. Run one additional business-critical selector before enabling schedule.

## Production rollout

1. Publish a normal release.
2. Deploy the package or image.
3. Run connector smoke and one manifest selector.
4. Validate row counts and freshness.
5. Enable schedule.

## Checklist

- [ ] Mindbox source secret exists.
- [ ] Sink secret exists.
- [ ] Canonical manifest is available.
- [ ] Connector smoke passed.
- [ ] Manifest smoke passed for at least one selector.
- [ ] Landing rows arrived.
- [ ] First scheduled run completed successfully.

## Related docs

- [Mindbox connector](mindbox.md)
- [Integration tests](testing/integration-tests.md)
