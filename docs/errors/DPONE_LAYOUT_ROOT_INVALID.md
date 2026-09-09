# DPONE_LAYOUT_ROOT_INVALID

**Audience:** platform engineers and repository maintainers.

`layout.root` is not a confined project-relative POSIX path, or discovery found
an unsafe object beneath it.

Use a path such as `workloads`. Absolute paths, `..`, backslashes, NUL bytes,
and symlink escapes are not accepted.

Inspect the public schema and current project:

```bash
dpone gitops schema show dpone.project.v1
dpone check . --format json
```

Set `layout.root` to a real project-relative directory such as `workloads`,
remove any symlink in that boundary, then verify:

```bash
dpone workload index
```

Exit `0` with schema `dpone.workload-index.v1` confirms the root is safe.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
