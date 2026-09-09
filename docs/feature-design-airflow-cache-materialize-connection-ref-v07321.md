# Feature design: Airflow cache materialization through a logical credential reference

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: Canonical Airflow Pack Recovery And Deployment
- Target release: v0.73.21
- Architecture authority: `docs/adr/0009-artifact-delivery-and-cache-materializer.md`
- Approval authority: maintainer-approved canonical Airflow pack recovery plan
Last verified: 2026-07-27

## Executive summary

`dpone airflow cache-materialize` already downloads one exact immutable Airflow
release/deployment and never resolves `latest` or `current`. Its CLI currently
allows only workload identity or a local registry emulator. The approved dev
Airflow deployment uses a read-only S3 Airflow connection projected as an
`AIRFLOW_CONN_*` environment variable, so operators cannot use the canonical
materializer without either a one-off URI parser or changing the cluster IAM
model.

This change allows the materializer to use the same bounded logical
`--connection-id` and injected credential resolver already certified for
`dpone airflow publish`. The domain service and `ArtifactRegistry` port do not
change. Credentials remain outside release/deployment artifacts, reports,
logs, cache files, and DAG parse. The measurable outcome is that a disposable
init container can restore an empty scheduler cache from exact IDs using a
read-only connection while keeping the scheduler image parse-safe.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Airflow operator | Restore an exact deployment after pod restart | Canonical CLI cannot use the existing read-only connection | One documented command restores without parsing a URI in shell |
| Security engineer | Keep credentials out of deployment evidence | A local wrapper would need to decode a connection URI | CLI accepts only a bounded logical name and redacts resolver failures |
| Platform engineer | Reuse the same registry composition | Publish and materialize expose different credential modes | Both commands inject the same `ArtifactRegistry` adapter |

Journey:

1. CI publishes an immutable release/deployment and promotion evidence.
2. Infra projects the read-only `AIRFLOW_CONN_*` value into a dedicated sync
   container.
3. The container invokes `cache-materialize` with exact release/deployment IDs
   and the logical connection name.
4. The existing resolver reads the credential only at command execution.
5. The materializer verifies and installs immutable local trees without
   activating `current`.
6. The existing CAS-protected `cache-sync` performs activation.
7. A retry revalidates the same exact IDs and is a deterministic no-op when the
   local trees are already complete.

## Scope

### In scope

- Add `--connection-id` and `--connection-type {airflow,env,vault}` to
  `dpone airflow cache-materialize`.
- Add a parse-safe `write_dpone_loader_ack(...)` provider helper so a
  control-plane can bind convergence to an exact completed parse.
- Reuse `ArtifactRegistryOptions`, `ObjectStorageConnectionResolver`, and the
  existing redacted error boundary.
- Keep all remote identities exact and content-addressed.
- Update CLI reference, compatibility guidance, runbook, and tests.

### Non-goals

- Reading mutable `latest`, branch, tag, or remote `current`.
- Activating local `current` during materialization.
- Adding credentials to manifests, release sets, deployment sets, evidence, or
  normal output.
- Installing full dpone in Airflow DAG parse processes.
- Adding a second artifact-registry abstraction or a deployment-specific S3
  adapter.

### Assumptions and constraints

- The sync container is a separate composition root and may install the
  object-storage extra; scheduler parsing still imports only the lightweight
  provider.
- The connection is read-only for the artifact prefix.
- `connection-id` keeps the existing bounded grammar
  `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`.
- Cloud-live certification is reported separately from deterministic tests.

## Public contract

### CLI

```bash
dpone airflow cache-materialize \
  --cache-root /opt/airflow/.dpone-cache \
  --release-id sha256:<release> \
  --deployment-id sha256:<deployment> \
  --environment dev \
  --artifact-registry-ref dpone-dev-artifacts \
  --registry-uri s3://example-data-bucket/dpone-artifacts/prod/example-workloads \
  --connection-type airflow \
  --connection-id s3_dpone_artifacts_reader \
  --max-total-bytes 536870912 \
  --format json
```

Exactly one access mode remains required:

- `--local-registry-root`;
- `--identity-mode workload_identity`;
- `--connection-id` with an optional explicit `--connection-type`.

URI-shaped, JSON-shaped, assignment-shaped, path-shaped, empty, or oversized
connection IDs are rejected before registry construction. Exit codes, stdout,
stderr, JSON schema, byte limits, immutable installation, and no-activation
semantics remain unchanged.

### Python API

No new domain API is introduced. `ArtifactRegistryOptions` already supports a
logical connection and remains the CLI composition model. Application services
continue to depend only on `ArtifactRegistry`.

The formal provider adds:

```python
write_dpone_loader_ack(
    report,
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
```

The helper reads only the already-local bounded index, writes atomically under
the separately mounted ACK root, rejects roots that overlap the cache, and
records no credentials or connection material. The cache writer consumes that
evidence through a read-only mount when planning retention. It exists because
unchanged DAG IDs can leave old SerializedDAG rows
visible after cache activation; visibility alone is not deployment
convergence.

### Artifacts and evidence

The existing `dpone.airflow-cache-materialize.v1` report is unchanged.
Credential source and values are not serialized. Failures use the existing
redacted `dpone.error.v1` boundary.

The additive `dpone.airflow_loader_ack.v2` local evidence contains exact
release/deployment IDs, the actual index SHA-256, UUIDv4 `activation_id`,
loaded/skipped DAG IDs, bounded error codes, `fatal`, and `acknowledged_at`.
The historical v1 shape has no activation identity. It remains
reader-compatible diagnostic evidence only and cannot certify convergence.

### Compatibility and migration

This is additive. Existing local-root and workload-identity invocations are
unchanged. Operators may replace wrapper-level credential parsing with a
logical connection reference. Rollback is to use workload identity or the
previous CLI version; immutable remote/local artifacts need no migration.

## Detailed algorithm

1. Parse exact release/deployment/environment/registry identity.
2. Parse exactly one access mode and validate a logical connection name before
   creating a credential provider.
3. Validate the local cache target and remote registry root.
4. Resolve credentials lazily through the selected provider.
5. Construct the existing object-storage registry adapter.
6. Download only declared content-addressed objects into a private staging
   directory with object and total byte limits.
7. Verify completion markers, sizes, hashes, release/deployment fingerprints,
   and the full deployment projection.
8. Atomically create-or-compare immutable local release/deployment trees.
9. Emit a redacted report with `activated=false`.
10. On retry, revalidate remote and local state; never infer another identity.

```text
parse exact IDs
  -> validate bounded logical ref
  -> inject ArtifactRegistry
  -> bounded staged download
  -> full projection verification
  -> immutable local install
  -> report (no activation)
```

State remains `requested -> validated -> materialized|no_op|failed`.
`cache-sync` owns the separate `materialized -> current` transition.

Edge cases:

- missing `AIRFLOW_CONN_*`: redacted dependency failure, no local activation;
- malformed connection ID: input error before resolver construction;
- partial remote release: failed report, no immutable local tree;
- checksum/size mismatch: failed report, staging removed, old `current` intact;
- process crash: private staging is disposable; retry starts from exact IDs;
- concurrent identical materialization: create-or-compare converges;
- connection with write permissions: materializer still exposes read/stat only
  through the registry operation path.

## Architecture

| Component | Change | Responsibility |
|---|---|---|
| Airflow artifact CLI parser | Additive | Expose logical credential mode for materialization |
| `ArtifactRegistryOptions` | Existing | Validate and compose one registry adapter |
| `ObjectStorageConnectionResolver` | Existing | Resolve Airflow/env/Vault credentials lazily |
| `AirflowArtifactMaterializer` | Unchanged | Verify and install exact immutable trees |

Dependency direction remains CLI composition root -> existing adapter -> port
-> application service. No connector SDK import is added to base/help paths.

Alternatives rejected:

- parsing an Airflow URI in Helm duplicates credential policy and redaction;
- storing AWS fields in deployment evidence leaks infrastructure binding;
- requiring new cluster workload identity blocks the current dev recovery;
- allowing remote `latest` recreates the incident's split control plane.

ADR 0009 requires a clarification because its previous CLI policy restricted
materialization to workload identity. The immutable identity and activation
boundaries are unchanged.

Expected implementation is under 30 Python SLOC in existing cohesive modules;
no new import-graph layer or module-size budget increase is expected.

## Market comparison

| System | Relevance | Decision | Source/date |
|---|---|---|---|
| Apache Airflow 3.x | Versioned DAG delivery and serialized scheduler contract | Adopt exact deployment identity and parse-local artifacts | [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html), 2026-07-27 |
| Astronomer Cosmos | Remote compiled manifest and cache behavior | Adopt compiled artifact; reject network access during parse | [Parsing methods](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html), 2026-07-27 |
| dlt, Airbyte, Fivetran | No Airflow scheduler cache activation contract | N/A | Product scope comparison, 2026-07-27 |
| Informatica, Pentaho, SSIS | No OSS Airflow parse-cache contract | N/A | Product scope comparison, 2026-07-27 |
| gusty, Apache Beam | DAG authoring/execution, not immutable scheduler cache promotion | N/A | Product scope comparison, 2026-07-27 |

## Measurable differentiation

```yaml
axis: deterministic restart recovery without parse-time network I/O
scenario: empty Airflow dagProcessor cache with an existing read-only S3 connection
baseline: temporary cache archive or wrapper-level URI parsing
metric: exact-ID verification and secret-bearing wrapper count
target: one verified release/deployment; zero wrapper credential parsers
procedure: publish, materialize, cache-sync, restart, materialize again
artifact: dpone.airflow-cache-materialize.v1 plus cache-sync evidence
limitations: live Yandex Object Storage proof requires the approved dev environment
```

## Security, privacy, and operations

- Only a logical connection name crosses CLI/evidence boundaries.
- Resolver exceptions remain redacted.
- The sync identity receives read/list only on the artifact root.
- Byte budgets remain mandatory and the local cache remains bounded.
- No remote operation occurs at DAG parse time.
- Last-known-good `current` is never modified on materialization failure.

## Test and certification plan

| Layer | Scenario | Expected artifact |
|---|---|---|
| Unit | Parser accepts each exclusive mode and rejects conflicts | pytest report |
| Contract | Invalid/secret-shaped logical IDs fail before resolver use | pytest report |
| Integration | Fake credential resolver -> local/S3 registry materialization | materialize JSON |
| Compatibility | Existing workload identity/local emulator tests stay green | pytest report |
| Live | Exact dev release restored through read-only Airflow env bridge | materialize/cache-sync evidence |

## Documentation plan

- Update cache sync runbook and CLI reference generator output.
- Amend ADR 0009 and compatibility notes.
- Add changelog entry and operator example.

## Rollout and rollback

1. Release `0.73.21` package set from one frozen commit.
2. Pin the dedicated sync image and provider packages to that version/digest.
3. Enable exact-ID materialization in dev only.
4. Preserve last-known-good `current`; rollback desired IDs or the sync image
   without rewriting artifacts.
5. Promote to production only after controlled restart recovery evidence.

## Agent execution plan

The integrator owns all OSS shared files. Cross-repository agents have disjoint
worktrees and may not edit this repository.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market research uses current official sources.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer changed status to `APPROVED`.

## Implementation evidence

Verified on 2026-07-27 from the `0.73.21` release candidate:

- focused materialization/resolver contract: `66 passed`;
- complete non-live suite outside the restricted socket sandbox:
  `7710 passed, 557 skipped`;
- Ruff check and format: passed;
- mypy: `690 source files`, passed;
- import rules, layer metrics, module size, Airflow public contracts and
  compatibility: passed;
- docs check, generated references, language contracts and strict MkDocs build:
  passed;
- four wheel/sdist pairs built and passed Twine validation;
- fresh Python 3.11 no-dependency package-set install: passed.

Live Yandex Object Storage materialization and controlled Airflow restart remain
the separate deployment-certification step. They are not represented as passed
by deterministic OSS evidence.
