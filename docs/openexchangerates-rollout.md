# OpenExchangeRates rollout runbook

This page describes the rollout pattern for the OpenExchangeRates connector.

## Canonical manifest

Use the OpenExchangeRates batch manifest from [examples/batch](https://github.com/PaulKov/dpone/tree/master/examples/batch) when available, or generate one with `dpone init`.

## Recommended table

```text
landing__openexchangerates__latest
```

## Required secrets

Store the vendor `app_id` in a supported secret backend.

## Rollout steps

1. Configure source and sink credentials.
2. Run connector smoke with a small symbol list.
3. Run manifest smoke.
4. Validate landing rows and base currency.
5. Enable schedule.

## Checklist

- [ ] Source secret exists.
- [ ] Sink secret exists.
- [ ] Manifest is available.
- [ ] Connector smoke passed.
- [ ] Manifest smoke passed.
- [ ] Landing rows arrived.
- [ ] First scheduled run completed successfully.

## Related docs

- [OpenExchangeRates connector](openexchangerates.md)
- [REST API](rest-api.md)
