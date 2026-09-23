# Publish a deployment with verified credential projection

This guide is for platform engineers upgrading a native workspace delivery lane.
Domain authors do not add credential mounts or repeat the control connection in
pipeline manifests. No secret values belong in any promotion artifact.

## Prerequisites and migration order

Install compatible deployment, provider, watcher and runtime readers before
enabling workspace authority. Provision the exact Secret keys through the
platform's existing secret-management system. Retain old immutable artifacts.
The workspace-enabled lane must emit deployment/index v6 and neutral promotion
evidence together; enabling the authority while retaining the legacy promotion
producer is deliberately rejected.

Non-workspace authority continues to accept its existing promotion evidence.
There is no missing-field fallback for a workspace-enabled authority. Do not
remove the authority or weaken the publication check to work around an error.

## Producer boundary

The canonical protected promotion producer first verifies its source, CI gates,
artifact attestations and pinned deployment. Call
`dpone.readiness.airflow_credential_promotion.build_credential_promotion_evidence`
with the protected typed authority and exact deployment, Airflow index,
credential-projection, binding-set and runtime-registry bytes. The helper checks
content addressing, mirrored subjects, projection paths, bindings and authority.
It performs no secret or network I/O and does not declare any CI gate passed.

Merge the returned `to_dict()` fields into the producer's existing receipt and
emit `schema_version: dpone.airflow-deployment-promotion.v1`. In addition to the
normal release/deployment, environment/scope, source/image/index, DAG inventory,
status and timestamp fields, this closed receipt requires:

| Field | Meaning |
| --- | --- |
| `credential_projection` | Exact `{artifact_ref, sha256, bytes}` descriptor |
| `workspace_authority_connection_ref` | Control ref derived from protected authority |
| `publish_authority_sha256` | Fingerprint of that same publication authority |

These fields contain references and digests, never URIs containing credentials.
The descriptor is content-addressed, bounded, and must match the verified v6
deployment and index. Do not construct it from mutable defaults or user input.

## Publication and execution

The existing desired-state preparation and publication commands consume this
receipt. Before creating a candidate or accepting a saved preparation they
compare its control ref and authority fingerprint with the currently loaded
protected authority. The exact receipt hash is already part of candidate
identity, so changing a descriptor requires a new preparation.

Activation independently verifies the actual artifact bytes. The base launcher
repeats projection and registry verification after ready-manifest validation,
checks the pointer-projected control ref supplied by the base composition root,
then pins that verified ref into the child environment. Init ready-state reuse
checks artifact integrity without requiring business credentials in init.

## Diagnose and recover

- Missing projection evidence: upgrade the producer and rebuild the deployment;
  do not add a guessed descriptor to an old receipt.
- Authority mismatch: verify environment, desired-object authority and control
  binding, then rebuild and re-prepare with one reviewed configuration.
- Modified bytes, path or size: restore the exact immutable artifact or publish
  a newly verified deployment; never edit ready-state evidence.
- Missing Secret key: repair provisioning, keeping values outside artifacts.

A successful preparation is not proof that the DAG executed. Require the
scoped live canary and terminal business result before claiming readiness.
Rollback requires readers compatible with any active workspace handover; do not
downgrade a protected lane to bypass admission.

See the [credential projection specification](feature-specs/deployment-credential-projection.md)
and [native credential provisioning guide](airflow-native-credential-projection.md).
