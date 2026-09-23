# Native Airflow credential projection

Audience: platform engineers provisioning native dbt deployments and operators
diagnosing credential-file delivery. Domain authors continue to declare logical
connection refs; they do not maintain Pod mounts or copy a control connection
into every catalog.

Implementation status: integration in progress. This document describes the
approved v6 contract, not release availability or live certification. Publication,
activation and base-launcher authority hooks must be connected and independently
reviewed before enabling the lane.

## Prerequisites and first deployment

1. Install compatible core, Airflow pack/provider and runtime readers before
   emitting deployment/index/init-fetch v6.
2. Configure the existing protected desired-state authority v2. Its
   `workspace_authority_connection_ref` remains the sole authored control
   selector. The build composition reads the same protected file selected by
   `DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE`.
3. Bind workload refs and that control ref through the environment binding-set.
   In the registry, each operator bridge entry has `resolver: airflow_connection`,
   `execution_mode: operator_bridge` and its original `connection_id`. Store no
   secret values in these documents.
4. Provision the existing namespace-local bridge Secret and watcher file mounts.
   Use the original connection ID to derive the exact key; a logical ref is not
   the key. The builder never reads Airflow Connections or Kubernetes Secrets.
5. Build the unchanged native release through the existing deployment build
   command. A native v2 release without protected authority fails closed.
6. Inspect the descriptor and review the restricted `credential-projection.json`
   metadata. Publish using the coordinated projection-bearing promotion evidence,
   activate only after watcher provisioning, and manually run a synthetic
   workload before normal delivery.

## Exact mapping example

Logical `warehouse` may bind registry `warehouse_runtime` whose source ID is
`sql_target_login`. The Secret key is `AIRFLOW_CONN_SQL_TARGET_LOGIN`, not
`AIRFLOW_CONN_WAREHOUSE_RUNTIME`. The base resolver reads
`/run/secrets/dpone/airflow-connections/warehouse_runtime/uri`.

Two registry aliases may intentionally use the same source ID and Secret key.
They receive separate alias directories. Case/punctuation normalization that
makes different source IDs collide is rejected. Only the selected workload refs
and its control ref enter the Pod; unrelated registry entries are not mounted.
Existing Vault KV resolvers keep their runtime authentication and are not
converted into bridge volumes.

## Ownership and verification

The deployment owns the projection; portable release packs keep their existing
bytes and empty native connection projection. Its content-addressed descriptor
is mirrored in deployment, index and runtime plan. Source-ID/key, membership,
path or authority changes produce another deployment identity. Credential-value
rotation does not; new Pods observe the current Secret values. SubPath mounts
do not promise in-place rotation for already-running Pods.

The provider verifies local metadata and creates read-only base-only mounts.
Artifact-fetch init containers and XCom sidecars do not receive target/control
credentials. Init-fetch carries the non-secret projection document; base verifies
it again before connecting. Ready manifests remain unchanged because the exact
plan and deployment transitively bind the projection.

## Troubleshooting and recovery

`DPONE_RUNTIME_CREDENTIAL_PROJECTION_REQUIRED` means the native lane lacks its
protected authority or sealed projection. Complete the coordinated lane upgrade;
do not inject fallback env values or manually patch the release pack.

`MISMATCH` means coordinate, authority, digest, membership or resolver-path parity
failed. Rebuild from matching immutable inputs. Preserve the failed deployment
and evidence for diagnosis; do not rewrite sealed files.

`INVALID`, `UNSUPPORTED` and `LIMIT_EXCEEDED` identify malformed metadata,
unsupported resolver combinations or a bounded-closure limit. Fix the source
metadata rather than weakening validation. Limits are 256 refs per workload,
1024 projection entries and 1 MiB projection bytes; plan size remains 16 KiB.

Kubernetes missing-Secret/key events indicate provisioning failure. Inspect names
and key membership through approved operational tools without dumping values.
Repair provisioning and retry unchanged mappings; mapping changes need a new
deployment. Rollback selects a previously verified compatible deployment and
does not retire workspace guards or reverse SQL.

Next: [design and test matrix](feature-specs/deployment-credential-projection.md),
[ADR 0071](adr/0071-deployment-owned-credential-projection.md), and
[Airflow integration](airflow-pack-provider.md).
