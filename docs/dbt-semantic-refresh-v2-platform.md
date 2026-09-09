# Evaluate semantic refresh V2 platform behavior locally

This platform-engineer how-to covers the 0.74 local diagnostic preview,
deployment planning, the local campaign, and the control-schema upgrade. It
does not activate a production route. Authors should use the
[V2 author journey](dbt-semantic-refresh-v2.md); incidents belong in the
[operations runbook](dbt-self-service-runbook.md#recovery-by-failure-boundary).

## Platform prerequisites

Before activation, provide:

- complete baseline receipts and one deployment-wide target owner;
- SQL Server writer/DDL ACL proof, allowlist, external-job inventory, and
  organizational exclusivity attestation;
- ongoing UTC assurance for each `datetime2(6)` event-time key;
- create-only versioned artifact storage and retention evidence;
- a certified exact-UUID ClickHouse predecessor-retention plan/apply controller
  before production activation; this controller is not shipped in the current
  preview, so predecessor generations are retained and production activation is
  blocked rather than silently accumulating or deleting them;
- exact transport-codec certification;
- per-operation and aggregate resource budgets;
- live fault-injection certification for the exact MSSQL, ClickHouse, Airflow,
  Kubernetes, Vault, artifact-store, driver, and runtime versions.

Skipped, mocked, stale, or inaccessible live checks remain `UNVERIFIED`.
Installing the 0.74 preview or migrating its schema does not activate
production. Both the certified exact-UUID retention controller and current
exact-environment live/fault evidence are mandatory in a future release. In
0.74 the shipped guard is unconditional; neither evidence nor operator input
can turn it into production authority.

The protected DDL-assurance issuer must take its epoch from the installed
control database, not an operator argument:

```python
from dpone.adapters.semantic_refresh_mssql_ddl_epoch import (
    MssqlSemanticRefreshDdlEpochObserver,
)

ddl_epoch = MssqlSemanticRefreshDdlEpochObserver(
    platform_mssql_connection_factory,
    control_schema="dpone_control",
).observe()
```

Bind that positive value into the signed `DDL_FREEZE` runtime-assurance
receipt. Admission carries it into the plan; the dbt mutation transaction
re-observes it while holding the machine shared lock. Any intervening DDL
requires a new catalog proof, assurance receipt and plan.

Use one synchronized published line that contains semantic-refresh V2. Replace
`<DPONE_VERSION>` only with that published version; a source-tree version is
not install authority until its artifacts are available from the configured
package index:

```bash
DPONE_VERSION="<DPONE_VERSION>"
pip install \
  "apache-airflow==3.3.0" \
  "apache-airflow-providers-dpone==${DPONE_VERSION}" \
  "dpone-airflow-pack==${DPONE_VERSION}" \
  "dpone[semantic-refresh-airflow]==${DPONE_VERSION}"
```

Install the approved Microsoft ODBC Driver. Do not substitute `dpone[full]`.
The DAG processor reads only the verified local index and constructs lazy
factories; it must not access MSSQL, Vault, S3, or ClickHouse at parse time.
The governed runtime image carries the exact closed `dbt-dpone` package at
`/opt/dpone/runtime/dbt-dpone`, verifies its canonical digest while building,
and binds that path as `package_source_root` in worker composition. Authors do
not install or override these platform macros.

## Verify that production activation is unavailable

Version 0.74 deliberately has no production activation procedure. The public
application root rejects both initial and successor authority persistence, and
the canonical MSSQL activation service uses the same fail-closed guard before
physical mutation. The public worker composition uses that guard before
admitting a new DagRun from any preserved receipt. The Python exception is the
complete public error contract; there is no CLI exit code or stdout/stderr
contract for this boundary.

The supported check is the application runtime, not a lower-level receipt or
adapter. It must raise before calling any injected store, verifier, or MSSQL
capability:

```python
from dpone.app.semantic_refresh_activation_composition import (
    SemanticRefreshProductionActivationUnavailableError,
    build_semantic_refresh_activation_runtime,
)

activation = build_semantic_refresh_activation_runtime(
    mssql_connection_factory=platform_mssql_connection_factory,
    release_deployment_verifier=protected_release_deployment_verifier,
    release_template_verifier=protected_release_template_verifier,
    authority_store_ref=protected_authority_store_ref,
)
try:
    activation.persist_deployment_authorities(
        template_pack=protected_template_pack,
        deployment_subject=protected_deployment_subject,
        plan_bundle=protected_plan_bundle,
        route_certification=protected_route_certification,
        runtime_assurances=protected_runtime_assurances,
        persisted_at=activation_authority_persisted_at,
    )
except SemanticRefreshProductionActivationUnavailableError as exc:
    assert exc.code == "DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE"
else:
    raise AssertionError("dpone 0.74 must not activate Semantic Refresh V2")
```

Do not catch this exception and fall through to a lower-level service or
adapter. A test-only injected allow guard exists solely in local integration
fixtures and is not platform authority. Planning remains useful for preview;
durable deployment-authority persistence and physical activation remain
blocked.

Build the run-neutral projection with
`build_semantic_refresh_dag_projection()` and publish its descriptor through
the authenticated deployment index only inside the disposable preview harness.
A self-digest is not authority. The following loader shape is retained as a
future platform contract, not as a 0.74 production procedure:

```python
from dpone.app.semantic_refresh_airflow_application import (
    build_semantic_refresh_airflow_application,
)
from platform_semantic_refresh_runtime import dependencies

application = build_semantic_refresh_airflow_application(**dependencies)
loaded = application.load_dags(
    globals(),
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    raise RuntimeError("semantic-refresh deployment index could not be loaded")
```

`dependencies` contains lazy factories and protected authorities, never
plaintext credentials. The first dbt task supplies the real DagRun identity;
no scheduler value, XCom, or environment default is admission authority.

## Recover without reactivating the deployment

This is a future certified-route contract, not an executable 0.74 production
procedure. In 0.74, both initial and successor persistence raise
`DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE`. Keep an incident on
the released V1 recovery path. The remaining details support local fault tests
and review of a future release.

Follow the [runbook procedure](dbt-self-service-runbook.md#recovery-by-failure-boundary)
only within that scope.
A failed-precommit successor must retain the exact predecessor release,
deployment, pre-release bundle, package, model closure, scope, route receipt,
and runtime-assurance bytes. Corrected SQL or renewed authority bytes require a
separately governed deployment.

Future recovery uses `plan_deployment_authorities()` followed by a guarded
`persist_planned_deployment_authorities()` with one retained typed authority
and timestamp. Once a future certified guard exists, this creates another immutable receipt under
`(deployment_id, plan_bundle_sha256)` and deliberately does not repeat initial
baseline/head/guard activation. There is no revision or `latest` lookup.

`COMMIT_UNKNOWN` blocks cleanup and replacement. Successor guard acquisition
also requires the exact failed-scratch absence acknowledgement and five
`RELEASED` allocation records. The actual DagRun worker performs the one
canonical replacement admission; recovery later exact-replays its persisted
binding.

## Run the local implementation campaign

This campaign is an opt-in diagnostic, not a release or production promotion
authority. First provision the disposable services named below and confirm
their health through your local platform harness:

- SQL Server 2022 with ODBC Driver 18 and the semantic-refresh fixture database;
- ClickHouse at the configured `DPONE_IT_CH_HTTP_URL` and a writable test
  database;
- the MinIO/KES object-storage fixture required by the cross-provider case;
- k3d context `k3d-dpone-semref-v2` with the test Vault deployment; and
- Airflow 3.3 from the locked all-extras environment.

Keep the repository clean because dirty source is deliberately reported as
`UNVERIFIED`. Run from the repository root and publish into a unique ignored
generation rather than the source-controlled historical example:

```bash
CAMPAIGN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
uv run python tools/semantic_refresh_local_campaign.py \
  --output ".dpone/generated/semantic-refresh-v2/${CAMPAIGN_ID}/local-docker-evidence.json"
```

Success exits `0`, writes the same canonical JSON to stdout and the requested
path, and writes nothing to stderr. Invalid arguments exit `2`; a failed,
skipped, partial, or dirty campaign exits `1` after writing a `FAIL` or
`UNVERIFIED` report. Use `--list` to print the closed check IDs without running
them. `--check CHECK_ID` is diagnostic only and cannot produce overall PASS
evidence. Reusing an existing output path is rejected rather than overwriting
prior evidence.

A complete local matrix has `status: PASS` and still records
`production_certification: UNVERIFIED`. A disabled/skipped live check is
`UNVERIFIED`, never PASS. Retain the generated report outside the source tree
or upload it as an access-controlled CI artifact; never hand-edit it or commit
it as current evidence.

The cross-provider check begins with an admitted SQL Server operation, seals
exact Parquet through KES-backed MinIO, publishes through the production
ClickHouse HTTP adapter, and persists the terminal summary to SQL Server.
Other checks cover Vault/Kubernetes authority, the exact dbt overlay, and real
Airflow 3.3 serialization.

## Upgrade control schema v20/v21 to v22

Version 21 changes the activation-authority primary key from
`deployment_id` to `(deployment_id, plan_bundle_sha256)`. It preserves rows and
permits a create-only recovery-plan receipt under the same deployment. It does
not introduce `latest`. Version 22 adds create-only same-DagRun attempt
continuation-receipt v1 storage plus a singleton monotonic DDL epoch and database DDL
trigger. The dbt transaction holds the corresponding shared application lock;
unrelated DDL requires the exclusive lock and advances the epoch.

Receipt v1 supports only the immediate successor try and is not automatic
retry authority. No continuation receipt v2 ships in 0.74; any broader holder
chain requires a separately approved post-0.74 contract and migration.

1. Quiesce semantic-refresh workers and schedules.
2. Take the protected SQL Server backup. Record schema version, PK columns,
   row count, and receipt digests without exporting receipt JSON.
3. Continue only from v20/deployment-only PK, exact v21/composite PK, or exact
   v22/composite PK plus continuation/DDL objects.
4. Apply through the matching runtime:

   ```python
   from dpone.adapters.semantic_refresh_mssql_schema import (
       MssqlSemanticRefreshSchemaMigration,
   )

   MssqlSemanticRefreshSchemaMigration(
       platform_mssql_connection_factory,
       control_schema="dpone_control",
   ).apply()
   ```

5. Require v22, exact composite key order, unchanged activation row
   count/digests, one positive DDL epoch row, the database DDL trigger, the
   continuation table, and a successful second `apply()` proving idempotency.

On `DPONE_SEMANTIC_REFRESH_ACTIVATION_AUTHORITY_KEY_MIGRATION_FAILED` or any
SQL error, keep admission stopped and require the exact pre-migration version,
key shape, and rows to remain unchanged. Never repair the key/receipts by hand.
Escalate with dpone version, stable code, redacted SQL error, version row, PK
metadata, row counts, and digest set. Use database restore only if transaction
rollback itself was not clean.

The detailed checklist is in the
[runbook](dbt-self-service-runbook.md#control-schema-v20v21-to-v22).

Rollback disables new V2 activation and restores the previous immutable V1
deployment pointer. Retain all V2 journals/evidence until governed cleanup is
permitted.
