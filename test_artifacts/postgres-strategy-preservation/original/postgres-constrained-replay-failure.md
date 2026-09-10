# Existing PostgreSQL internal-query full-refresh defect

Status: **FAIL**, observed in the approved local minikube environment on source
`e501cf87a4e482960d4445dbe27d43bdbdd8064a`. This is outside the approved
hooks/resources feature's load-algorithm scope. It remains unresolved.

Controller `f2f3ff22-cd23-4558-a33b-243387762700` created source and destination
tables with `id integer PRIMARY KEY, label text NOT NULL`. The first strict
Airflow run passed, ran its separate hook once and copied all three expected
rows. The second run of the same release/deployment failed schema validation:
the target columns had become nullable after the first run.

The default `truncate_insert` strategy routes `InternalQueryArtifact` directly
to `PostgresInternalQueryLoader.load`. That loader replaces the target using
`CREATE TABLE AS` and drops its backup. The replacement loses the original
primary key and `NOT NULL` constraints. The next run correctly rejects the
requested nullability tightening. This also means the first successful run
does not preserve the target constraints promised by default truncate/insert.

Evidence:

- `controller/f2f3ff22-cd23-4558-a33b-243387762700/results/resources-success.json`
- `controller/f2f3ff22-cd23-4558-a33b-243387762700/results/resources-replay/08-observed-run.json`
- `pod-logs/3c793540-6763-419c-bd61-e15ade12515b/base.log`
- Read-only independent execution-path review by `live_k8s_execution_map`.

Relevant implementation:
`src/dpone/runtime/sinks/strategies/postgres/postgres_base.py` and
`src/dpone/runtime/sinks/strategies/postgres/internal_query_loader.py`.
The documented default is in `docs/postgres.md`.

The bounded hooks/resources matrix uses nullable synthetic columns for both
source and destination and retains exact row equality, row-count and hook-count
checks. It does not disable schema validation, repair constraints between
runs, or certify constrained PostgreSQL full-refresh replay.

Suggested separate correction: send default full-refresh internal queries
through the existing staging/materialization and truncate handler; reserve
replacement for explicitly requested exchange behavior. A live regression
must run twice and verify target identity, primary key and nullability remain
intact in addition to row equality. Do not claim general production readiness
for this route until that correction and its evidence are complete.
