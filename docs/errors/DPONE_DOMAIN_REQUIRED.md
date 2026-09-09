# DPONE_DOMAIN_REQUIRED

**Audience:** pipeline authors.

The project uses domain-first authoring, but `dpone init pipeline` did not
receive `--domain`.

Create or identify the owning domain, then rerun:

```bash
dpone init domain <domain> \
  --owner-team <team> \
  --owner-contact <contact> \
  --approver-team <github-team>
dpone init pipeline <pipeline-id> --domain <domain> --recipe <recipe>
```

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
