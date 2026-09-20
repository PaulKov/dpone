# ADR 0070: Non-secret runtime-authority payloads use a closed immutable v5 wire

- Status: Accepted
- Date: 2026-09-20

## Context

ADR 0068 requires each protected runtime process to invoke one image-installed
authority adapter before sensitive work. The initial KPO projection carries the
adapter's external configuration through a Kubernetes Secret and intentionally
rejects inline values because Secret contents may be confidential.

Some adapters need only small, safe-to-persist configuration. Requiring a Secret
for those bytes introduces mutable cluster state and operational authority that
is not inherent to the configuration. Digest binding supplies integrity but does
not supply confidentiality, and deployment/index/runtime-plan artifacts plus Pod
specifications are visible to their existing readers.

## Decision

Keep deployment/index/runtime-plan v4 and its `kubernetes_secret_volume` source
frozen. Add v5 contracts with one closed alternative named `immutable_payload`.
It contains canonical standard Base64, decoded byte count, and canonical SHA-256
over 1..4,096 exact decoded bytes. The existing 16 KiB total runtime-plan bound
does not change. Old readers reject v5 explicitly.

Immutable payload bytes are classified as safe-to-persist opaque configuration
or ciphertext. They must not contain plaintext credentials, tokens, private
endpoints, tenant identifiers, personal data, or private policy. Confidential
bytes continue to use the v4 Kubernetes Secret source.

The producer verifies an explicit expected digest while performing one bounded
binary file read. The provider independently validates the closed source and
includes it in the hash-bound v5 runtime plan. The pod uses a provider-owned,
memory-backed, size-limited `emptyDir` at the existing fixed authority path.
Init-fetch validates and atomically materializes the file before authority or
registry access. Base opens it with bounded/no-follow semantics and independently
checks size and digest before its separate authority call. Neither path creates,
reads, updates, nor deletes a deployment-specific Secret or ConfigMap.

Errors are stable and redacted. The implementation never logs, emits through
XCom/evidence, or includes payload bytes in exceptions. It introduces no generic
source plugin or caller-selected command, image, encoding, path, or limit.

## Consequences

Safe non-secret configuration can be delivered deterministically without a
mutable cluster object. Same bytes yield the same deployment, plan, and pod
projection. Retries use a fresh Pod-local volume; concurrent pods share no state.

Payload bytes remain observable to principals that can read build artifacts,
scheduler metadata, or Pod specs. Operators must use Secret mode for plaintext
secret material. The init mount is writable only to materialize the verified
file; base is read-only. Core, Airflow pack, provider, and runtime image must be
upgraded together for v5 and rolled back as an exact set.

## Related contracts

- [Approved feature specification](../feature-specs/kpo-runtime-authority-immutable-payload.md)
- [ADR 0024: strict Airflow init-fetch wire](0024-airflow-executable-init-fetch-wire-boundary.md)
- [ADR 0068: image-installed development runtime authority](0068-image-installed-development-runtime-authority.md)
- [Secret projection specification](../feature-specs/kpo-runtime-authority-projection.md)
