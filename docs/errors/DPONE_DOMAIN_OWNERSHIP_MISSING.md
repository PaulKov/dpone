# DPONE_DOMAIN_OWNERSHIP_MISSING

**Audience:** domain owners and pipeline authors.

The requested domain has no `ownership.yaml`. dpone blocks pipeline creation so
an unowned workload cannot appear in check, preview, release, or publish
results.

Run:

```bash
dpone init domain <domain> \
  --owner-team <team> \
  --owner-contact <contact> \
  --approver-team <github-team>
```

Then repeat `dpone init pipeline`. Placeholders such as `TODO` are rejected.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
