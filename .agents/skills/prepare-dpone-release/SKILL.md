---
name: prepare-dpone-release
description: Audit a named dpone version and frozen commit using scoped R1-R9 evidence, or verify an already published version through the read-only controller receipt. Separate readiness, publication authority, and retrospective observation.
---

# Prepare a dpone release

1. Read `docs/release.md` and `docs/agent-release-protocol.md` for current
   authority. Require operation, version, release type, frozen source commit,
   changed-scope inventory, and reviewed controller revision.
2. For retrospective verification, follow the controller command in the runbook
   with the original run/artifact identities and unaltered ZIP. Use a fresh
   output directory per observation; the verifier requires an overall
   successful publisher run. Report its PASS/FAIL/UNVERIFIED observation;
   do not dispatch publication to obtain proof or reuse a stale install log.
   For preparation, classify R1-R9 applicability and requirement sources first.
3. Delegate independent read-only audits for CLI/run, nested identity,
   route/strategy matrix, contracts, docs/CJM, Airflow/dbt, packaging/security,
   and recovery/observability as scope requires.
4. Record each item as PASS, FAIL, SKIP, N/A, or UNVERIFIED with command/workflow,
   environment, timestamp, observed result, artifact, and owner.
5. Use an executable checklist only when its certification scope applies and
   underlying evidence exists. Checklist booleans do not create proof. Do not
   require unrelated live campaigns, GitHub Release creation, GHCR images, or
   CI-shadow backlog closure for a PyPI-only observation.
6. Use `docs/agent-templates/release-evidence-report.md` for the consolidated report.
7. Any blocking FAIL or required SKIP/UNVERIFIED yields NO-GO. N/A needs rationale.
   Preserve mandatory source checks and merge receipts; the controller does not
   execute them. A readiness GO is not separate publication authorization.
8. A new source/controller revision invalidates affected evidence and decisions.
   Stop on upload uncertainty or partial publication; never automatically retry,
   enable skip-existing, rebuild retained evidence, or restore a second publisher.
