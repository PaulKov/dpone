# SimilarWeb rollout runbook

This page describes the rollout pattern for the SimilarWeb Website Keywords connector.

## Canonical manifest

Use [examples/batch/landing_similarweb_api.batch.yaml](https://github.com/PaulKov/dpone/blob/master/examples/batch/landing_similarweb_api.batch.yaml) as the canonical manifest.

## Recommended table

```text
landing__similarweb__keywords
```

## Credit and scheduling policy

SimilarWeb live checks may consume shared vendor credits. Keep live jobs manual unless the owning team explicitly approves scheduled certification.

## Development rollout

1. Build or publish a snapshot package.
2. Configure source and sink secrets.
3. Run connector smoke.
4. Run manifest smoke for `keywords` with a small domain set.
5. Validate landing rows and quality gates.

## Production rollout

1. Publish a normal release.
2. Deploy the package or image.
3. Run connector smoke and one manifest selector.
4. Validate duplicate checks and row-count thresholds.
5. Enable schedule.

## Checklist

- [ ] SimilarWeb source secret exists.
- [ ] Sink secret exists.
- [ ] Canonical manifest is available.
- [ ] Connector smoke passed.
- [ ] Manifest smoke passed.
- [ ] Landing rows arrived.
- [ ] Quality gates are green.
- [ ] First scheduled run completed successfully.

## Related docs

- [SimilarWeb connector](similarweb.md)
- [Quality metrics](quality-metrics.md)
- [Integration tests](testing/integration-tests.md)
