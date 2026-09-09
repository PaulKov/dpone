# Configure tenant hygiene for a release

The source readiness workflow scans frozen Git blobs and eight candidate
archives. Repository secret `TENANT_HYGIENE_POLICY` contains the maintainer's
deny list, not a PyPI credential. Configure it under GitHub repository settings,
**Secrets and variables → Actions**. Never commit the real policy or print it.

## Policy formats

Existing newline-delimited policies retain exact, case-sensitive UTF-8 byte
matching. Each nonempty line is a literal; there are no comments or regexes.
A version header explicitly selects structured JSON. Existing literal lines,
including lines beginning with `{`, retain their meaning. The header family
`#!dpone.tenant-hygiene-policy.` is reserved; unknown versions fail closed:

```text
#!dpone.tenant-hygiene-policy.v2
{
  "schema": "dpone.tenant-hygiene-policy.v2",
  "terms": ["ExampleTenant", "example_pipeline"],
  "jira_prefixes": ["ABC"],
  "detectors": ["rfc1918"]
}
```

Replace fictional terms with the approved inventory directly in GitHub. All
four fields are required; unknown or duplicate fields are rejected. Terms must
be nonempty. Prefixes and detectors may be empty lists. Each list allows 1024
entries, each entry 256 Unicode characters; the complete policy is bounded to
1 MiB. Invalid or unavailable policy means `UNABLE_TO_CERTIFY`.

## Matching rules and coverage

Terms use Unicode NFC and case folding, then remove ASCII spaces, tabs,
newlines, carriage returns, underscores, hyphens, dots, slashes and backslashes.
The remaining term must contain only letters or digits. Matching is substring
based, including inside hostnames, filesystem paths and configuration values.
Other characters, including invalid UTF-8 bytes, remain barriers. Arbitrary
Unicode lookalikes and encoded/obfuscated strings are outside this contract.

Jira prefixes contain ASCII letters. Matching permits the same separators
between prefix letters and requires at least one separator before numeric issue
digits. A key cannot be embedded inside a longer word. A bare prefix is not a key.

The optional `rfc1918` detector validates complete dotted IPv4 addresses against
exact RFC1918 networks. Loopback, documentation networks, invalid octets and
longer dotted strings do not match. Private addresses in examples and tests
also produce findings; their location does not exempt them.

Provide concrete cloud project/namespace values and mobile application IDs in
`terms`. Company-name terms also match identifiers containing those names.
Unspecified, unrelated identifiers are outside verified coverage; the scanner
cannot infer a private inventory from generic identifier syntax.

Corporate paths embedded in compiled Python bytes are scanned with UTF-8
surrogate escape. No bytecode is deserialized or executed. Compiled artifacts
without matching values are not automatically classified as corporate material.

## Execution and failure handling

Source paths, blob bodies, archive member paths and bodies share one matcher.
Structured policies read each size-bounded archive member completely before
matching, preventing missed Unicode/separator matches at read boundaries. The
32 MiB member limit remains; decoded and normalized representations require
additional bounded memory. Legacy policies retain streaming literal matching.

Frozen source identity, no-follow archive access, replacement detection, archive
limits, report schema and exit codes are unchanged. Reports contain safe codes
and paths, never matched values. Protected paths are redacted. A finding returns
`FAIL` / exit 2; invalid or unsafe input returns `UNABLE_TO_CERTIFY` / exit 3.

After integrating the scanner, dispatch `source-release-readiness.yml` on
protected `master`. Retain both successful jobs and their original artifacts as
described in [Agent release protocol](../agent-release-protocol.md). Local scans
are diagnostic and do not replace CI-owned release evidence. Investigate without
printing sensitive snippets; remove accidental material through normal review
rather than relaxing the policy to make a release pass.

## Implementation and validation scope

This additive correction implements the approved
[tenant hygiene design](../feature-design-runtime-connection-authority-and-hygiene-v0732.md#tenant-hygiene-scan)
and the maintainer's request for case/separator variants and private addresses.
The policy module owns parsing/matching; the scanner retains Git/archive
boundaries and reports. The CLI loads its fixed sibling in isolated stdlib-only
execution. No runtime API or publication authority changes.

Regressions cover legacy behavior, Cyrillic, case folding, separators, malformed
policies, address and Jira boundaries, protected paths, compiled-path bytes and
archive read boundaries. Tests use fictional identifiers. Record actual checks
and remaining coverage gaps in release evidence.
