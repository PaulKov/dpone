# CBR rollout runbook

This page describes the rollout pattern for the CBR public XML connector.

## Canonical manifest

Use [examples/batch/landing_cbr_api.batch.yaml](https://github.com/PaulKov/dpone/blob/master/examples/batch/landing_cbr_api.batch.yaml) as the canonical manifest.

## Recommended table

```text
landing__cbr__exchange_rates
```

## Credentials

The CBR source is public and does not require source credentials. The target sink still requires normal warehouse credentials.

## Development rollout

1. Build or publish the package under test.
2. Configure the target sink.
3. Run public connector smoke.
4. Run manifest smoke for a small date range.
5. Validate landing rows.

## Production rollout

1. Publish a normal release.
2. Deploy the package or image.
3. Run public connector smoke.
4. Run one manifest selector.
5. Enable the schedule after validation.

## Smoke command

```bash
uv run pytest tests/integration -m integration_live -k cbr
```

## Checklist

- [ ] Sink secret exists.
- [ ] Canonical manifest is available.
- [ ] Public connector smoke passed.
- [ ] Manifest smoke passed.
- [ ] Landing rows arrived.
- [ ] First scheduled run completed successfully.

## Related docs

- [CBR connector](cbr.md)
- [Integration tests](testing/integration-tests.md)
