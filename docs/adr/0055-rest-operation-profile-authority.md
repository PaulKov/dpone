# ADR 0055: REST protocol semantics are platform-owned operation profiles

## Status

Proposed, 2026-08-30. Runtime implementation is blocked until this ADR and the
governed REST bulk-delivery specification are approved.

## Context

Allowing each workload to author HTTP methods, paths, acknowledgement states,
duplicate behavior, retry rules, polling, response extraction, and timeout
semantics would recreate bespoke connector code as YAML. It would also let a
data author weaken security or claim idempotency without endpoint certification.

Connection configuration alone is insufficient. It owns authority and secret
references but should not be overloaded with the semantics of every operation
available at that service. Workload intent, connection security, and protocol
behavior have different owners and change cadences.

The product scope must also be honest. One-request batch delivery and one-submit
async jobs do not implement reverse ETL object change capture, per-record retry,
multi-step bulk protocols, resumable uploads, or callbacks.

## Decision

Split authority into:

1. a platform-owned immutable connection binding for authority, auth provider,
   credential ref, TLS, proxy, network policy, certified stable remote
   principal/account/resource namespace, and binding semantics digest;
2. a platform-owned immutable `operation_profile_ref` for HTTP semantics,
   payload family, acknowledgement, duplicate proof, retry fence, polling,
   result extraction, deterministic codec, hard limits, bounded overrides,
   timeout vector, distributed controls, retention requirements, and
   certification;
3. workload-owned intent for logical resource, mapping, mutation kind, source
   scope, explicit generation source, and permitted planning preferences.

The compiler resolves these references offline into one immutable deployment
plan. Runtime recovery uses the plan pinned by the operation journal, never a
current manifest or mutable registry. Profile versions are immutable and have a
content digest. Deprecation does not alter an active release.

The profile exposes three distinct digests:

- `operation_semantics_digest` binds method/path/query/header/body-envelope,
  acknowledgement, mutation fence, and verification semantics;
- `execution_policy_digest` binds retry, timeout, rate, concurrency, retention,
  and observability policy;
- `profile_contract_digest` binds both digests plus schema/version metadata and
  is pinned in the complete parent plan.

`mutation_intent_digest` uses operation semantics and the complete canonical
rendered request, not execution policy. Therefore a policy-only change cannot
cause an intent conflict, while recovery still uses the exact pinned policy.
The rendered request binds ordered scalar/file multipart parts, deterministic
filename/boundary/content metadata, and sealed payload digest; wire limits
measure the complete emitted envelope.

V1 supports only `sync_batch` and `async_bulk_job`. Unsupported protocol
families fail compilation. V1.1 may add object add/change/remove and per-record
result/quarantine contracts. V1.2 may add bounded protocol graphs for
create/upload/finalize, resumable ranges, presigned URLs, and callbacks. State
schemas reserve compatible continuation types without pretending they are
implemented.

OpenAPI may bootstrap a draft profile for maintainer review. It never grants
runtime authority or certification automatically. Response-derived URLs remain
forbidden in V1 except typed relative paths or absolute `Location` values that
canonicalize to the profile's exact HTTPS origin and an allowlisted relative
receipt path.

V1 authentication capabilities are bearer, allowlisted API-key header, HTTP
Basic, OAuth 2.0 client credentials, and mTLS. API-key query authentication is
forbidden by production default; a profile-specific exception requires
security approval and proven URL redaction across client exceptions, traces,
proxy/WAF/access logs, and receiver telemetry. HMAC, SigV4, delegated-user
OAuth, browser flows, and response-derived credentials are future capabilities.
A mutable deny-only revocation authority may block new submissions for a
profile or binding without changing a pinned plan or preventing safe read-only
observation/reconciliation.

Secret values are resolved fresh in each process into non-serializable objects.
The immutable binding contains references and semantics, not token or
certificate bytes. Rotation and OAuth refresh preserve mutation identity when
the binding semantics are unchanged.

Remote identity assurance is explicit. `receiver_attested` uses a certified
receiver claim/identity endpoint. `platform_attested` is allowed when no such
endpoint exists, but requires dual approval, immutable evidence, expiry, and
periodic recertification. Expiry blocks new mutations while preserving safe
observation and reconciliation.

Mutation-kind policy is profile-owned. `partition_replace` permits governed
correction revision and overlap fencing. `incremental_append` forbids correction
revision and duplicate applied exact scopes by default. Opt-in requires
certified receiver dedup/upsert, a natural idempotent key, replacement
semantics, or an explicit compensating-correction contract.

The profile or its certified receiver-resource binding owns
`effect_conflict_group`, joining every operation kind and local namespace that
can mutate one physical receiver effect. The receiver duplicate fence uses
`receiver_mutation_key = H(operation_namespace, delivery_key)`; it never uses
payload digest or Airflow identity. Connection binding semantics explicitly
cover authority, stable principal/account/resource namespace, authentication
semantics, TLS policy identity, and network policy identity.

## Consequences

- Self-service manifests are short and safe, with strong editor/catalog UX.
- Platform teams take explicit ownership of endpoint semantics and certification.
- A service with many operations can reuse one connection and multiple profiles.
- Protocol changes require a profile version and deployment, making drift
  visible and recoverable.
- Connector breadth grows more deliberately than arbitrary request builders,
  but every production behavior has one reviewable authority.
- The design can adopt standard profiles such as synchronous response,
  `Location` header, Google long-running operation, custom JSON job, Salesforce
  bulk, and resumable upload in their appropriate versioned family.

## Validation

- Closed-schema tests reject low-level request semantics in workload YAML.
- Bounded-override tests prove authors cannot exceed profile limits or weaken
  guarantee, authority, TLS, retry, or retention.
- Profile digest and version are stable across encoding order and bind every
  semantic field; property tests prove policy-only changes preserve mutation
  intent while semantic/envelope/principal changes do not.
- Recovery tests continue from the pinned profile after registry deprecation or
  a newer workload release.
- Secret-sentinel tests cover plan, journal, trigger kwargs/events, XCom, logs,
  spans, metrics, exceptions, and evidence.
- Certification tests cover remote-identity assurance expiry, dual approval for
  platform attestation, query-key default denial/redaction exceptions, and
  append correction rejection.

## Related decisions

- [Feature design](../feature-design-rest-api-sink-v1.md)
- [ADR 0010](0010-binding-set-and-credential-resolvers.md)
- [ADR 0027](0027-single-runtime-connection-authority.md)
- [ADR 0030](0030-self-service-capability-studio-boundary.md)
- [ADR 0053](0053-rest-delivery-identity-effect-journal.md)
- [ADR 0054](0054-resumable-delivery-runtime-airflow.md)
- [Threat model](../rest-bulk-delivery-threat-model.md)
