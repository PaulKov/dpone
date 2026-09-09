# DPONE_PIPELINE_ID_DUPLICATE

**Audience:** pipeline authors and repository maintainers.

The same pipeline id exists in more than one project location. Pipeline ids are
project-wide identities in domain-first v1, so a duplicate would make selection,
evidence, and Airflow DAG ownership ambiguous.

Detect every conflicting authority:

```bash
dpone check . --format json
```

Choose a project-unique id and rerun the original scaffold with the same route
arguments:

```bash
dpone init pipeline <new-id> \
  --domain <domain> \
  --route <source>:<sink>:<strategy> \
  --from <source-ref> \
  --to <sink-ref> \
  --key <column>
dpone check <domain>/<new-id>
```

There is no automatic rename because references and evidence may depend on the
old identity. No files are created by the failed attempt. Escalate to a
reviewed authoring migration when the existing id must be retained.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
