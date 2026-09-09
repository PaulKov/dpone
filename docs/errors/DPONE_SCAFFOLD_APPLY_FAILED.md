# DPONE_SCAFFOLD_APPLY_FAILED

**Audience:** pipeline authors and repository maintainers.

A filesystem write failed after scaffold apply started. dpone attempted to
remove every operation-owned file and returned the complete rollback receipt.
The original operating-system exception is not exposed because it may contain
sensitive path or credential-shaped text.

Rerun the same command with `--format json` and inspect:

- `changes`: each planned file is `rolled_back`, `preserved`, `failed`, or
  `recovery_required`;
- `rollback_journal.entries`: project-relative files or directories that could
  not be proven removed. `verify_and_remove` entries include the captured
  device/inode and require an identity check; directory entries also require an
  emptiness check;
- `rollback_issues`: redacted compensation failures;
- `recovery_artifacts`: confined files that must be inspected manually;
- `recovery_required`: whether another init attempt is unsafe.

When `recovery_required` is `true`, stop concurrent writers and reconcile every
journal entry before retrying. Preserve bytes owned by another writer. Never
delete a path merely because it appeared in the original plan or because its
name matches a journal entry.

When `recovery_required` is `false`, operation-owned files were rolled back;
repair the filesystem or permissions issue and rerun the original init command.

[Domain-first error overview](index.md) ·
[Domain-first operations](../domain-first-operations.md)
