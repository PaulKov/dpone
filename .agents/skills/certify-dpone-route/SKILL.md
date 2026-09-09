---
name: certify-dpone-route
description: Certify a dpone source-to-sink route and load strategy using capability metadata, contract tests, live evidence, reconciliation, state/evidence ordering, retries, and understandable artifacts. Use for route work and release gate R4.
---

# Certify a source-to-sink route

1. Identify source, sink, load strategy, schema/type profile, and declared
   capabilities from authoritative metadata.
2. Classify the matrix cell as supported, unsupported with reason, experimental,
   or certification-required. Do not infer support from an implementation method.
3. Run unit and contract coverage for negotiation, validation, mapping, and
   unsupported combinations.
4. Move real rows in the approved integration environment and verify counts or
   reconciliation, types/schema, strategy semantics, deterministic state/evidence
   ordering, retry/resume, and recovery/quarantine.
5. Verify artifacts are readable, machine-parseable, attributable to the exact
   commit/environment, and free of secrets.
6. Keep mocked contract evidence and live certification as separate statuses.
7. Update the source-sink matrix, connector/strategy docs, runbook, and release
   evidence. A skipped live run remains SKIP/UNVERIFIED.
