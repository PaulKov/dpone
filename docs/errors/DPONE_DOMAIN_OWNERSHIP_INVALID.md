# DPONE_DOMAIN_OWNERSHIP_INVALID

**Audience:** domain owners.

The domain ownership file is malformed, unsafe, or disagrees with its directory
name. It must use schema `dpone.domain-ownership.v1` and contain non-empty
`owner.team`, `owner.contact`, and `approvers.github_team`.

Fix the existing authoring source and run `dpone check .`. dpone never
overwrites an ownership file with different user content.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
