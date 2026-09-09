# Domain-first Airflow project

Use domain-first layout when pipeline ownership should be visible in the
repository tree and a team should be able to add a pipeline without editing a
shared domain catalog.

This journey creates one domain and one MSSQL to ClickHouse pipeline. It uses
only authoring files, performs no network or secret-store calls, and does not
require Airflow Python.

## Before you start

This page is for a data engineer creating a first domain-owned pipeline. Follow
the [installation guide](installation.md), verify `dpone --version`, and use a
new empty working directory. The feature is currently documented under
`Unreleased`; until its patch release is published, install the reviewed source
checkout explicitly:

```bash
export DPONE_REVIEWED_REVISION="<exact-release-tag-or-40-character-commit>"
test "$DPONE_REVIEWED_REVISION" != "<exact-release-tag-or-40-character-commit>"
git clone --no-checkout https://github.com/PaulKov/dpone.git
cd dpone
git fetch origin "$DPONE_REVIEWED_REVISION"
git checkout --detach "$DPONE_REVIEWED_REVISION"
test "$(git rev-parse HEAD)" = "$(git rev-parse "$DPONE_REVIEWED_REVISION^{commit}")"
uv sync --frozen
export PATH="$PWD/.venv/bin:$PATH"
dpone init domain --help
mkdir ../dpone-domain-first-quickstart
cd ../dpone-domain-first-quickstart
```

The release or change owner supplies the exact reviewed revision; do not use a
moving branch name for this source-checkout path.

After the patch is published, the normal `pip install dpone` path replaces
these source-checkout steps. A supported build exposes `dpone init domain
--help` and `--layout domain-first`. Run every journey command from the new
empty quickstart directory. The route is credential-free until a platform
engineer binds the generated logical connection references.

## Five-command journey

The product target is completion in 15 minutes or less after installation and
access to an empty project directory. Automated tests prove that the commands
and artifacts work, but they are not human-usability evidence. The target
remains `UNVERIFIED` until a milestone study records at least five new users
and at least 80% finish without help.

```bash
dpone init project --airflow --layout domain-first
dpone init domain crm \
  --owner-team data-crm \
  --owner-contact crm@example.com \
  --approver-team data-platform
dpone init pipeline orders_daily \
  --domain crm \
  --route mssql:clickhouse:incremental_merge \
  --from mssql_dev:dbo.orders \
  --to clickhouse_dev:analytics.orders \
  --key order_id
dpone init dag DAG__crm__orders__refresh \
  --domain crm \
  --schedule "0 6 * * *" \
  --pipeline orders_daily
dpone check crm/orders_daily
dpone airflow preview orders_daily
```

For Airflow-eligible pipelines (`airflow.enabled: true`, the default), `init
pipeline` also registers catalog membership in
`.dpone/config/domains/<domain>.yaml` under the `workloads:` block. The write
is idempotent: rerunning the same command after a successful init is a no-op.
If the catalog already registers the same pipeline id with a different
`manifest:` path, or the catalog cannot be patched safely, the command fails
closed **before** any pipeline files are written. When membership fails after
the scaffold (for example a concurrent catalog edit), operation-owned scaffold
files are rolled back automatically; fix the catalog and rerun the command.

Multi-task DAGs are authored next to pipelines under
`workloads/<domain>/dags/<dag_id>.yaml`. Do not edit
`dpone_workloads/gitops/domains/` for new work; dual-read keeps legacy catalogs
readable during migration.

Every command returns exit code `0`. The two validation commands finish with:

```text
dpone check: OK
dpone airflow preview: OK
- deployment: preview (not runnable)
- airflow index: .dpone-cache/current/airflow-index.json
```

Any non-zero exit stops the journey. Follow the structured error's `docs` and
`fix` lines before continuing.

Run the generated credential-free behavior test when you want an executable
local proof:

```bash
dpone test orders_daily
```

The route command resolves through the same capability catalog used by
`dpone recipe list`; it is not a second recipe table. Unsupported routes fail
before any file is written.

## Files you own

The project contains:

```text
dpone.yaml
.dpone/
  config/
    project.yaml
    domains/
      crm.yaml
workloads/
  crm/
    ownership.yaml
    pipelines/
      orders_daily/
        pipeline.yaml
        tests/
          pipeline.test.yaml
          fixtures/
            input.jsonl
dags/
  dpone.py
environments/
  dev/
    binding-set.yaml
    credential-runtime.yaml
platform/
  connection-registries/
    dev.yaml
```

| Path | Responsibility |
| --- | --- |
| `dpone.yaml` | Selects `domain_first`, the confined authoring root, and project-wide pipeline ids. |
| `workloads/crm/ownership.yaml` | Domain owner and approver metadata. |
| `workloads/crm/pipelines/orders_daily/pipeline.yaml` | The only editable source for this pipeline. |
| Colocated `tests/` | Hermetic behavior contract and fixture. |
| `.dpone/config/project.yaml` | Committed system workload-set root used by CI reconcile. |
| `.dpone/config/domains/crm.yaml` | Committed membership maintained by `init pipeline`; do not hand-edit it. |
| `dags/dpone.py` | Generated provider loader; commit it but do not edit it. |
| `environments/dev/` | Non-secret development binding and credential-runtime references. |
| `platform/connection-registries/dev.yaml` | Non-secret logical connection registry stub. |

Do not create or edit `domains/*.yaml` in a domain-first project. Do not commit
`.dpone-cache/**` or a generated workload index.

Preview creates immutable release/deployment artifacts and the local current
pointer below `.dpone-cache/`. They are generated, content-addressed, and must
not be edited or committed.

## CI reconcile handoff

After committing the authoring and committed system files above, CI uses the
generated workload-set root rather than an individual domain catalog:

```bash
dpone gitops airflow reconcile \
  --workload-set .dpone/config/project.yaml \
  --all-workloads \
  --env dev \
  --output-dir .dpone/gitops
```

The output under `.dpone/gitops/` is ephemeral. Platform CI next runs
`dpone gitops airflow release-materialize`, followed by the strict
`dpone airflow build` / `publish` flow in
[Airflow self-service architecture](../airflow-self-service-architecture.md#runtime-artifact-delivery).

## Project-wide operations

Target one pipeline by id or by domain-qualified shorthand:

```bash
dpone check orders_daily
dpone check crm/orders_daily
```

Select all pipelines in a domain for project-wide validation or preview:

```bash
dpone check . --select 'domain:crm'
dpone airflow preview . --select 'domain:crm'
```

Selection consumes the same discovery snapshot used by individual lookup and
immutable preview materialization. A pipeline is not rediscovered by a
different algorithm for build or publish.

## Next steps

- Learn how exact-depth discovery, public Python APIs, and the ephemeral
  workload index work in [Domain-first discovery and CI](../domain-first-discovery-ci.md).
- Use [Domain-first operations and recovery](../domain-first-operations.md) for
  `--no-airflow`, custom roots, compatibility, and failure recovery.
- Bind logical connection references using the
  [configuration reference](../reference/configuration.md).
- Review [Airflow cache sync and recovery](../airflow-cache-sync.md) before
  production deployment.
- Use [workload selectors](../airflow-self-service-selectors.md) for bounded
  multi-pipeline CI and preview scopes.
