# Strict Airflow environment bridge

Status: APPROVED
Last verified: 2026-09-24
Approval basis: maintainer explicitly requested restoration of execution-time
Airflow Connection environment delivery without Secret creation or RBAC changes.
Target release: next compatible patch; publication remains separately gated.

## Problem and journey

Platform maintainers already use Airflow Connections and can create Pods, but
need not have permission to create Kubernetes Secrets. Strict compact delivery
currently permits only Secret-volume projection. Analysts must not maintain
credential inventories or alter model/DAG authoring to use either transport.

The platform selects `connection_projection.mode: env`. The producer closes
the declared connection entries over actual workload dependencies. An executing
operator reads only those Connections, applies declared nonsecret URI overrides,
and injects values into the runtime container. The runtime resolves its verified
registry using those values, without Airflow or Kubernetes clients.

## Contract and scope

- Add explicit closed `env` projection for strict compact releases.
- Entries contain connection_ref, registry_connection_ref and connection_id;
  canonical AIRFLOW_CONN names are derived from validated physical IDs.
- Keep secret_values=false and payload_format=airflow_connection_uri.
- No secret_name, mount_path, fields, cleanup policy or literal values in env
  projection. Reject ambiguous normalized names and incompatible aliases.
- Retain scheme/database/query override closure and all pack/signature/image,
  launch-pin, DQ, outcome and deployment identity checks.
- Published runtime registry uses a dedicated `airflow_env` URI resolver with
  connection_id metadata, never an Airflow API call or generic env_var fallback.
- Preserve registry-owned database/schema over URI defaults, existing credential
  policy metadata, and latest/workload-start behavior. No version-pin claim.
- Existing volume and legacy unsafe modes retain compatibility. No implicit
  migration or fallback. Native composed credential-projection releases remain
  unchanged; unsupported transport combinations fail explicitly.
- No CLI flags, warehouse/state algorithms, scheduling or retries change.

## Algorithm and failure semantics

1. Validate metadata, connection IDs and canonical environment-name collisions.
2. Close each workload's projection over its compiled dependencies; preserve
   existing alias and URI-override ambiguity checks.
3. Verify pack payloads before deriving deployment connection snapshots. Merge
   compatible projection entries; reject conflicting transports for one ref.
4. Generate runtime registry snapshots and fingerprint those exact bytes.
5. Parse/serialize DAGs without reading any credential values.
6. At execution only, resolve required Airflow Connections through an injected
   reader, apply overrides, register sensitive values with Airflow masking,
   and inject only into the base container. No Secret API call is made.
7. Keep init registry credentials separate; never propagate runtime credentials
   to init containers, sidecars, XCom or launch-pin/evidence payloads.
8. Runtime reads the exact canonical env variable and parses it through the
   existing Airflow URI parser. Missing/malformed values fail with redacted
   errors before connector I/O, without selecting another backend.
9. Existing Pod lifecycle, transaction, retry and replay mechanisms remain
   authoritative. Dispose execution-local values after execution; no additional
   persistent resource is created.

```text
verified packs -> closed projection -> fingerprinted runtime registry
execute -> read Connections -> normalize -> mask -> base-container env
runtime -> resolve declared env URI -> apply registry coordinates -> run
```

## Architecture and ownership

Provider owns closed projection validation and execution-time container
injection. Compiler owns dependency closure and verified deployment snapshots.
Runtime owns URI resolution behind existing credential services. No import-time
I/O, monkey patch, new service or SQL procedure is introduced.

Integrator owns shared schemas, release/deployment composition, documentation
and changelog. Delegated operator and credential-resolution writers have disjoint
paths and separate worktrees. Tests use only synthetic names and endpoints.
Keep modules within docs/benchmarks/quality_budgets.yml; extract focused helpers
instead of expanding oversized operator modules.

## Security and tradeoffs

Environment values are necessarily visible in the Kubernetes PodSpec to callers
with Pod-read access and potentially to cluster audit infrastructure. This is an
explicit platform choice, not equivalent to Secret-volume confidentiality.
Failure PodSpec logging is disabled; full URIs and secret components are masked.
No promise of protection from privileged cluster observers is made. Applications
must not print their environment. Source artifacts and evidence contain only
references; no hashes of secrets are produced.

## Current primary-source comparison

Apache Airflow Kubernetes provider 10.22.0 documents Pod environment configuration
and warns that environment secrets are visible to Pod observers. Adopt native
KPO execution and explicit exposure, reject blanket environment inheritance.
Source (checked 2026-09-24):
https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html
Cosmos uses the same Kubernetes execution boundary; no Cosmos behavior changes.
dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty and Beam are N/A: this
change is specific to an existing Airflow provider credential boundary, not a
connector or transformation comparison. No superiority claim is made.

## Verification and rollout

Extend existing focused contracts: mode validation, closure/collisions, compact
materialization, snapshot fingerprints, production/development URI resolution,
missing/malformed redaction, shared physical IDs with different catalogs, zero
parse-time reads, zero Secret API calls, base-only env injection and preserved
init credential refs. Test real provider logging and serialization where
available. Keep legacy volume regressions green. Target: zero Secret API calls
and zero sentinel credentials in logs/artifacts; live status remains UNVERIFIED
until exact-version canary evidence exists.

Document platform configuration and exposure model. No analyst-maintained lists
or workflow migration. Release through normal green checks, synchronously pin
provider/runtime, select env only for the authorized deployment, then canary.
Rollback selects the previous signed deployment; do not silently switch backends.
