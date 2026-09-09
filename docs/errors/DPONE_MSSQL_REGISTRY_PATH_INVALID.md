# DPONE_MSSQL_REGISTRY_PATH_INVALID

**Audience:** platform operators and GitOps authors.

dpone refused to load an MSSQL connection registry because the resolved path is
unsafe or unreadable.

## What broke

Usually one of:

1. The registry path contains a symlink component.
2. The resolved file escapes the repository root.
3. The candidate is not a regular file.
4. The file exceeds the bounded byte budget (1 MiB).
5. The environment name in `--env` contains path separators.

## Next step

1. Keep registries as regular files under exactly one of:
   - `.dpone/registry/connection-registries/<env>.yaml`
   - `platform/connection-registries/<env>.yaml`
2. Do not symlink registry directories or files into the repository.
3. Retry `dpone gitops airflow pack/reconcile --env <env>` after fixing the path.
