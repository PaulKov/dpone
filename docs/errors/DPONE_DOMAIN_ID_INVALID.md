# DPONE_DOMAIN_ID_INVALID

**Audience:** domain owners and pipeline authors.

The requested domain id is not canonical. Use 2–128 lowercase letters, digits,
`_`, or `-`, starting with a letter or digit. dpone reports a suggestion but
never silently renames the domain.

Rerun `dpone init domain <suggested-id>` with explicit `--owner-team`,
`--owner-contact`, and `--approver-team` values before creating pipelines.
If the error came from `dpone init pipeline --domain`, inspect the exact
options with `dpone init pipeline --help` and use the same canonical domain id.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
