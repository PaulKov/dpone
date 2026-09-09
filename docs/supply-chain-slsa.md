# SLSA release self-assessment

Purpose: document the current dpone release-artifact integrity posture against
the SLSA v1.2 Build Track, with explicit evidence and explicit non-claims.

Audience: maintainers, release auditors, security reviewers, and users who need
to verify where a published dpone distribution came from.

## Scope

This page applies to public release artifacts produced by
`.github/workflows/release.yml` for tags matching `v*.*.*`:

- wheels and source distributions under `dist/*.whl` and `dist/*.tar.gz`;
- local dpone supply-chain evidence under `test_artifacts/supply-chain/current/`;
- GitHub Artifact Attestations and verification receipts under
  `test_artifacts/supply-chain/github-attestations/`;
- PyPI publication and GitHub Release creation that happen after the
  attestation verification step.

It does not assess developer-local builds, historical releases, downstream
container images, user-built packages, or artifacts rebuilt outside the release
workflow.

## Current posture

| SLSA v1.2 Build Track concern | dpone release control | Evidence | Status |
|---|---|---|---|
| Build provenance exists | The release workflow runs `actions/attest-build-provenance` for every wheel and source distribution subject. | GitHub Artifact Attestations for `dist/*.whl` and `dist/*.tar.gz`. | Present for release workflow artifacts. |
| Provenance is verified before publication | The release workflow runs `gh attestation verify` for every wheel and source distribution before PyPI publish and GitHub Release creation. | JSON receipts in `test_artifacts/supply-chain/github-attestations/`. | Enforced in release workflow. |
| Source identity is constrained | Verification is scoped to the current repository, Git ref, and commit SHA. | `--repo "$GITHUB_REPOSITORY"`, `--source-ref "$GITHUB_REF"`, and `--source-digest "$GITHUB_SHA"`. | Enforced in release workflow. |
| Package metadata is checked | The release workflow runs `uv run twine check dist/*` from the synced project environment. | Release workflow logs and package metadata gate. | Enforced in release workflow. |
| Local supply-chain bundle exists | `dpone supply-chain attest` emits SBOM, provenance, local HMAC envelope, and bundle checksum evidence. | `test_artifacts/supply-chain/current/`. | Present as supplemental evidence. |
| Scorecard posture is visible | The OSSF Scorecard workflow publishes SARIF and public results. | `.github/workflows/scorecard.yml` and uploaded SARIF. | Advisory posture signal. |
| Agent governance controls are executable | `tools/agent_policy/governance_gate.py` validates agent policy inventory, red-team catalog, release attestations, Scorecard evidence, and this self-assessment. | `test_artifacts/agent-policy/agent_governance_gate.json`. | Enforced in CI and change-aware validation. |

No higher-level SLSA claim is made by this page. In particular, dpone does not
currently claim a specific SLSA Build Track level for every historical artifact,
does not claim SLSA Source Track coverage, and does not claim protection for
artifacts produced outside the GitHub release workflow.

## Release verification path

The release workflow follows this order:

1. Check out the tagged source.
2. Install a pinned `uv` runtime through `astral-sh/setup-uv`.
3. Build wheel and source distribution artifacts.
4. Run `uv run twine check dist/*`.
5. Generate dpone supply-chain evidence with `dpone supply-chain attest`.
6. Upload local supply-chain evidence with `if: always()`.
7. Generate GitHub Artifact Attestations for every wheel and source distribution.
8. Verify every artifact with `gh attestation verify`, constrained to the
   repository, ref, and commit SHA.
9. Upload verification receipts from
   `test_artifacts/supply-chain/github-attestations/`.
10. Publish to PyPI and create the GitHub Release only after verification
    succeeds.

If attestation verification fails, the release must stop before publication.
Operators should inspect the failed receipt, rebuild from the tagged commit, and
avoid reusing mismatched artifacts.

## Governance gate

Run the executable policy gate locally or in CI:

```bash
uv run python tools/agent_policy/governance_gate.py \
  --base-ref origin/master \
  --output test_artifacts/agent-policy/agent_governance_gate.json
```

The gate passes only when:

- repository-local agent policy inventory validates;
- the executable red-team scenario catalog validates;
- agent-control-surface changes are paired with red-team catalog validation;
- the release workflow contains GitHub Artifact Attestation generation and
  verification controls;
- the OSSF Scorecard workflow publishes SARIF and public results;
- this SLSA self-assessment exists and keeps explicit non-claims.

## Agent governance provenance

Agent-control PR evidence uses GitHub Artifact Attestations separately from
release publishing. CI signs the generated
`test_artifacts/agent-policy/agent_governance_gate.json` subject before
uploading the `agent-governance-gate` artifact. `Agent PR receipt` then
downloads the artifact archive, extracts that JSON file, and verifies its
attestation with `gh attestation verify` scoped to the dpone repository, the
`.github/workflows/ci.yml` signer workflow, GitHub's OIDC issuer, the SLSA
provenance predicate, and GitHub-hosted runner provenance.

This control proves where the governance evidence subject came from; it does
not by itself prove that the governance result is correct. The receipt still
validates artifact metadata, local archive fingerprint, JSON schema, `status:
PASS`, changed paths, and required governance checks before accepting the
evidence.

## Auditor checklist

For a release review, collect:

- tag name and commit SHA;
- release workflow run URL;
- `dist/*.whl` and `dist/*.tar.gz` hashes;
- `test_artifacts/supply-chain/current/supply_chain_attestation.json`;
- JSON files from `test_artifacts/supply-chain/github-attestations/`;
- `test_artifacts/agent-policy/agent_governance_gate.json`;
- OSSF Scorecard run or SARIF artifact;
- PyPI resolver visibility smoke output.

All evidence must come from the same commit or explicitly state why it is
`SKIP`, `N/A`, or `UNVERIFIED`. Stale or manually edited release evidence is not
a pass.

## References

- [SLSA v1.2 tracks](https://slsa.dev/spec/v1.2/tracks)
- [GitHub Artifact Attestations](https://docs.github.com/actions/security-guides/using-artifact-attestations-to-establish-provenance-for-builds)
- [OpenSSF Scorecard](https://scorecard.dev/)
- [OWASP Top 10 for LLM Applications 2025](https://genai.owasp.org/llm-top-10/)
- [OWASP Top 10 for Agentic Applications 2026](https://genai.owasp.org/resource/owasp-top-10-for-agentic-applications-for-2026/)

## Related pages

- [Supply-chain evidence](supply-chain.md)
- [Developer supply-chain guide](developer-supply-chain.md)
- [Release evidence](release-evidence.md)
- [Agent governance](agent-governance.md)
