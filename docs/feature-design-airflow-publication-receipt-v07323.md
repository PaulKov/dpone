# Feature design: trusted Airflow publication receipt

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Canonical Airflow Pack Recovery And Deployment
- Target release: v0.73.23
- Approval authority: user-approved canonical recovery plan
- Last reviewed: 2026-07-28

## Problem and outcome

The deployment build report intentionally redacts
`credential_runtime_ref`. A promotion consumer can therefore compare the
public build report with persisted `airflow-index.json`, but it cannot prove
that a hidden reference was part of the bytes verified in object storage.
An index descriptor emitted by the same build is self-attested and is not a
publication boundary.

The publisher must produce an independent receipt after remote read-back:

```text
validated local projection
  -> immutable create-or-compare
  -> bounded remote read-back and SHA-256 verification
  -> exact root-artifact commitment
  -> promotion gate compares local bytes with the receipt
```

The outcome is fail-closed exact promotion: changing the persisted index,
deployment, hidden credential reference, or their descriptors without a
matching verified publication receipt blocks activation.

## Public contract

`dpone airflow publish` exposes two explicit modes:

- `--publication-mode compatible` is the backward-compatible default. It
  preserves the historical `dpone.airflow-artifact-publish.v1` report and
  registry port behavior.
- `--publication-mode exact` accepts only a matched
  `dpone.deployment-set.v2` / `dpone.airflow-deployment-index.v2` projection,
  reads back every object, and emits `dpone.airflow-artifact-publish.v2`.

Historical v1 evidence remains readable but cannot authorize a new exact
promotion. The shared Airflow CI component always selects `exact`.

The v2 report adds:

```yaml
verified_objects: 35
publication_commitment:
  schema: dpone.airflow-publication-commitment.v1
  verification_mode: remote_readback_sha256
  registry_scope_id: sha256:...
  projection_verified: true
  release:
    object_key: releases/sha256-.../release-set.json
    sha256: sha256:...
    bytes: 1234
  deployment:
    object_key: deployments/dev/sha256-.../deployment.json
    sha256: sha256:...
    bytes: 2345
  airflow_index:
    object_key: deployments/dev/sha256-.../airflow-index.json
    sha256: sha256:...
    bytes: 3456
```

Failures in exact mode use the same v2 schema with
`publication_commitment: null`. They never claim a verified commitment.
Compatible-mode failures retain the v1 report shape.

## Algorithm and failure semantics

1. In exact mode, require the matched v2 deployment/index schema pair before
   registry or credential I/O.
2. Validate and freeze the complete local publication inventory.
3. Create each immutable object or compare it with the existing object.
4. Download every object through the bounded registry port.
5. Verify exact size and SHA-256 against the frozen snapshot.
6. Increment `verified_objects` only after successful read-back.
7. Publish completion markers last, using the same read-back rule.
8. Reconstruct the exact remote projection from read-back bytes and run the
   canonical release/deployment projection validator against it.
9. Build the root commitment from the already verified frozen inventory.
10. Return success only when all objects, both completion markers, the registry
   scope, and the reconstructed projection are verified.

A timeout, corrupt read-back, missing object, or checksum mismatch returns a
failed report with partial counters and no commitment. Re-running is safe:
create-or-compare converges on the same immutable bytes.

The promotion consumer additionally reads exact local `deployment.json` and
`airflow-index.json`, verifies both against the receipt, and requires their
hidden `credential_runtime_ref` values to match. It does not reimplement the
deployment fingerprint algorithm.

## Architecture and compatibility

- `dpone.runtime` owns immutable publication and receipt models.
- The artifact registry port remains connector-neutral. Exact publication
  requires the narrow `ArtifactRegistryAuthority.authority_scope_id` capability
  in addition to immutable read/write; compatible mode keeps accepting the
  historical port and its root-only `scope_id`.
- `registry_scope_id` is a credential-free canonical fingerprint of provider,
  actual SDK endpoint authority, account where applicable, bucket/container,
  and immutable registry root. Exact publication uses the endpoint observed
  from the constructed storage client; a caller-supplied endpoint label is not
  accepted as proof of authority.
- Vendor SDKs remain adapters and are not imported by base/help paths.
- Exact authority observation is implemented by the S3, GCS, Azure Blob, and
  local adapters through one thin `ObjectStorageEndpointAuthority` capability.
  Azure endpoint evidence removes SAS query material but retains an Azurite
  routing path. Endpoint normalization is lazy and is not evaluated by
  compatible publication.
  Custom object-storage adapters remain available in compatible mode, but
  exact publication fails closed until their SDK-selected endpoint is exposed.
  Migration requires implementing that capability; rollback uses compatible
  mode and never trusts caller-supplied endpoint text.
- The shared Airflow CI kit consumes the receipt but does not duplicate dpone
  release/deployment hashing policy.
- In the internal deployment flow the trust origin is the artifact produced by
  the exact protected GitLab publish job. Infrastructure validates source
  project, pipeline/job identity, exact job SHA, and current protected branch
  head before it accepts promotion evidence.
- The publication receipt is point-in-time content proof. It does not prove
  that any Airflow cache switched to that content. Cache activation retains its
  separate UUIDv4 `activation_id`, ACK v2, and REST convergence contract.
- Existing callers keep the v1 output and historical registry protocol through
  the default compatible mode. New exact promotion requires the explicit exact
  mode and the matched v2 projection pair.
- Cache materialization is a dual reader for matched v1/v1 and v2/v2 schema
  pairs. Mixed schema wires fail closed.

## Test and certification plan

- Unit: compatible v1 preservation, exact-mode rejection of v1 and mixed
  projections before registry I/O, v2 schema, exact root descriptors, all-object read-back, corrupt
  read-back, semantic remote projection drift, wrong registry scope, endpoint
  authority drift with an unchanged bucket/root before first write, unsafe
  endpoint rejection, first-party cloud adapter authority observation, partial
  progress, retry/no-op, and v1 historical validation.
- Consumer: modified index with recomputed build descriptor but unchanged
  receipt is blocked; index/deployment hidden-reference drift is blocked;
  missing or malformed commitment is blocked.
- Integration: local object-storage publish, materialize, and exact promotion.
- Live dev: immutable S3 publish followed by exact activation and Airflow REST
  convergence for all expected DAG IDs.

Skipped live checks remain `UNVERIFIED`.

## Market relevance

Apache Airflow versioned DAG bundles and serialization motivate exact,
locally parsed deployment artifacts. Astronomer Cosmos motivates compiled
artifact caching without parse-time remote I/O. Neither defines dpone's
object-storage publication receipt, so the adopted extension is an explicit
dpone control-plane contract:

- [Airflow DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html)
- [Airflow DAG serialization](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html)
- [Cosmos caching](https://astronomer.github.io/astronomer-cosmos/optimize_performance/caching.html)

dlt, Airbyte, Fivetran, Informatica, Pentaho, SSIS, gusty, and Apache Beam are
`N/A`: they do not expose a comparable Airflow scheduler artifact publication
and activation contract.
