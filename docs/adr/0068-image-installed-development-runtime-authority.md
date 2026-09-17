# ADR 0068: Development runtime authority is image-installed and rechecked per process

- Status: Accepted
- Date: 2026-09-17

## Context

ADR 0066 separates authenticated development artifact delivery from workload
execution. The remaining runtime boundary must admit an exact selected workload
before registry, credential, source, or command access without embedding any
deployment-specific policy implementation in public dpone.

Airflow init and base containers are separate processes. A successful init check
cannot prove that authority is still current when the base container starts.
Passing a receipt through CLI, environment, XCom, or a writable file would also
let the scheduling surface select or replay execution authority.

## Decision

Project a literal development-authority requirement into a new closed,
hash-covered Airflow index and runtime plan version. Exact release, deployment,
image, workload, and execution identities form the adapter request. Both init-fetch and
pack-exec independently invoke exactly one adapter installed through a fixed
Python entry-point group in the digest-pinned runtime image.

Core defines the request/result protocol and performs environment, time,
revocation epoch, selected runtime-subject, and fetched projection validation. The adapter owns
external policy lookup and authentication. Missing, multiple, unloadable,
mismatched, expired, or revoked authority fails closed with a stable redacted
error before sensitive runtime operations.

Ordinary deployments retain existing index and plan versions and do not load the
adapter. No CLI or environment option selects the implementation.

## Consequences

Private installations can implement policy without forking public core or
placing organization details in public artifacts. Runtime image construction and
digest pinning become part of the development execution trust boundary.

Each successful Pod lifecycle performs two external checks. This small cost is
intentional: revocation between init and base start must take effect. Adapter
availability is required for development execution but has no effect on ordinary
production runtime behavior.

Old readers reject the new closed schemas. Core, Airflow pack, provider, and
runtime image must therefore be upgraded as an exact release set.

## Privacy boundary

Public code and tests contain only generic protocols, stable errors, and synthetic
fixtures. Organization names, private repositories, endpoints, tables, SQL,
credentials, policy documents, and operational evidence remain in separately
distributed private adapter packages and downstream deployments.

## Related contracts

- [Approved feature specification](../feature-specs/development-runtime-authority.md)
- [ADR 0024: strict Airflow init-fetch wire](0024-airflow-executable-init-fetch-wire-boundary.md)
- [ADR 0035: production deployment attestation](0035-airflow-production-artifact-attestation.md)
- [ADR 0066: development delivery vs execution](0066-development-workspace-batch-federation.md)
