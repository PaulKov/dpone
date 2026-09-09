# DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT

**Audience:** CI maintainers and platform engineers.

The accepted baseline no longer matches the raw digest and semantic
fingerprint reviewed by the approval job, or bootstrap found that a baseline
already exists. The same error also protects a candidate or baseline outside
the captured project root, a replaced project-root inode, a symlinked parent,
or a concurrent promotion.

Preserve the current baseline and failed result. Fetch the protected current
baseline again, regenerate the candidate and impact report against those exact
bytes, repeat protected approval, and rerun `dpone workload promote`. If the
project root or a parent is a symlink, restore a real confined directory before
creating new evidence. Never overwrite the baseline with `mv`, shell
redirection, or a force flag.

[Domain-first discovery and CI](../domain-first-discovery-ci.md) ·
[Domain-first error overview](index.md)
