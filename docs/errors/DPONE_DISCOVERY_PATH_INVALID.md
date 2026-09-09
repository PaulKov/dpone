# DPONE_DISCOVERY_PATH_INVALID

**Audience:** pipeline authors and platform engineers.

Domain-first discovery encountered a symbolic link or unsafe path at an
authoring boundary. Discovery is exact-depth and no-follow so project sources
cannot escape the configured project root.

Inspect the exact rejected path:

```bash
dpone check . --format json
```

Replace a link with a real project directory, or move a misplaced visible
entry to the documented exact-depth path. Do not copy its contents through an
unreviewed link. Verify recovery:

```bash
dpone workload index
dpone check .
```

Both commands must exit `0`. Escalate if the reported path is required by an
approved custom layout.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
