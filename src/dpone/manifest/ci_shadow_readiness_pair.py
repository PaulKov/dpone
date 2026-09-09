"""Fail-closed, descriptor-confined transaction for one readiness report pair."""

from __future__ import annotations

import os
import secrets
from collections.abc import Callable
from pathlib import Path
from typing import cast

from dpone.manifest.ci_shadow_readiness_pair_codec import (
    PairMember,
    decode_journal,
    digest,
    encode,
    identity,
    journal_record,
    member,
    validate_inputs,
)
from dpone.manifest.ci_shadow_readiness_pair_fs import MAX_BYTES as _MAX_BYTES
from dpone.manifest.ci_shadow_readiness_pair_fs import ReadinessPairError
from dpone.manifest.ci_shadow_readiness_pair_fs import desired_owned as _desired_owned_fs
from dpone.manifest.ci_shadow_readiness_pair_fs import identity_is_current as _idempotent
from dpone.manifest.ci_shadow_readiness_pair_fs import open_directory as _open_directory
from dpone.manifest.ci_shadow_readiness_pair_fs import optional as _optional
from dpone.manifest.ci_shadow_readiness_pair_fs import pair_lock as _pair_lock
from dpone.manifest.ci_shadow_readiness_pair_fs import prior_or_absent as _prior_or_absent_fs
from dpone.manifest.ci_shadow_readiness_pair_fs import publish_identity as _publish_identity
from dpone.manifest.ci_shadow_readiness_pair_fs import recover_transaction as _recover_transaction_fs
from dpone.manifest.ci_shadow_readiness_pair_fs import remove_snapshot as _remove_snapshot
from dpone.manifest.ci_shadow_readiness_pair_fs import replace_if_current as _replace_if_current_fs
from dpone.manifest.ci_shadow_readiness_pair_fs import require as _require
from dpone.manifest.ci_shadow_readiness_pair_fs import require_identity as _require_identity
from dpone.manifest.ci_shadow_readiness_pair_fs import require_same_snapshot as _require_same_snapshot_fs
from dpone.manifest.ci_shadow_readiness_pair_fs import same_inode as _same_inode
from dpone.manifest.ci_shadow_readiness_pair_fs import write_new as _write_new
from dpone.manifest.confined_atomic_exchange import (
    AtomicExchange,
    AtomicExchangeUnsupported,
    get_native_atomic_exchange,
)
from dpone.manifest.confined_files import ConfinedFileError, ConfinedFileSnapshot

_JOURNAL = ".dpone-readiness-pair.json"
_IDENTITY = ".dpone-readiness-pair-identity.json"
_LOCK = ".dpone-readiness-pair.lock"
_JOURNAL_MAX_BYTES = 8_192


def write_pair(
    directory: Path,
    *,
    json_name: str,
    markdown_name: str,
    json_bytes: bytes,
    markdown_bytes: bytes,
    report_kind: str,
    subject_commit_sha: str,
    phase_hook: Callable[[str], None] | None = None,
) -> None:
    """Durably replace the private pair, retaining authenticated prior leaves."""

    _validate_inputs(json_name, markdown_name, json_bytes, markdown_bytes, report_kind, subject_commit_sha)
    exchange = _exchange_or_raise()
    with _open_directory(directory) as parent_fd, _pair_lock(parent_fd, _LOCK):
        _recover(parent_fd, exchange)
        directory_stat = os.fstat(parent_fd)
        receipt = identity(
            report_kind,
            subject_commit_sha,
            json_name,
            markdown_name,
            json_bytes,
            markdown_bytes,
            directory_device=directory_stat.st_dev,
            directory_inode=directory_stat.st_ino,
        )
        if _idempotent(parent_fd, _IDENTITY, receipt, max_bytes=_JOURNAL_MAX_BYTES):
            return
        if _optional(parent_fd, _IDENTITY, max_bytes=_JOURNAL_MAX_BYTES) is not None:
            raise ReadinessPairError("READINESS_OUTPUT_CONFLICT")
        token = secrets.token_hex(16)
        json_member = _prepare_member(parent_fd, json_name, json_bytes, token)
        markdown_member = _prepare_member(parent_fd, markdown_name, markdown_bytes, token)
        record = journal_record(receipt, json_member, markdown_member, "PREPARED")
        journal = _create_journal(parent_fd, record)
        _notify(phase_hook, "journal_durable")
        _notify(phase_hook, "before_json_exchange")
        _require_same_snapshot_fs(parent_fd, _JOURNAL, journal, max_bytes=_JOURNAL_MAX_BYTES)
        _install_member(parent_fd, json_member, exchange)
        _notify(phase_hook, "json_replaced")
        record, journal = _transition(parent_fd, record, "JSON_REPLACED", exchange)
        _notify(phase_hook, "json_state_durable")
        _notify(phase_hook, "before_markdown_exchange")
        _require_same_snapshot_fs(parent_fd, _JOURNAL, journal, max_bytes=_JOURNAL_MAX_BYTES)
        _install_member(parent_fd, markdown_member, exchange)
        _notify(phase_hook, "pair_replaced")
        _notify(phase_hook, "before_pair_state")
        record, journal = _transition(parent_fd, record, "PAIR_REPLACED", exchange)
        _notify(phase_hook, "pair_state_durable")
        _require_desired(parent_fd, json_member)
        _require_desired(parent_fd, markdown_member)
        _publish_identity(parent_fd, _IDENTITY, receipt, max_bytes=_JOURNAL_MAX_BYTES)
        _notify(phase_hook, "pair_durable")
        record, journal = _transition(parent_fd, record, "COMMITTED", exchange)
        _notify(phase_hook, "committed_state_durable")
        _notify(phase_hook, "before_cleanup")
        _cleanup(parent_fd, record, journal)


def recover_pair(directory: Path) -> None:
    """Recover only authenticated records; preserve every uncertain artifact."""

    exchange = _exchange_or_raise()
    with _open_directory(directory) as parent_fd, _pair_lock(parent_fd, _LOCK):
        _recover(parent_fd, exchange)


def _recover(parent_fd: int, exchange: AtomicExchange) -> None:
    try:
        _recover_journal_transition(parent_fd, exchange)
        snapshot = _optional(parent_fd, _JOURNAL, max_bytes=_JOURNAL_MAX_BYTES)
        if snapshot is None:
            return
        record = decode_journal(snapshot.content)
        json_member, markdown_member = member(record["json"]), member(record["markdown"])
        state = cast(str, record["state"])
        if state == "COMMITTED":
            _require_desired(parent_fd, json_member)
            _require_desired(parent_fd, markdown_member)
            _require_identity(
                parent_fd, _IDENTITY, cast(dict[str, object], record["identity"]), max_bytes=_JOURNAL_MAX_BYTES
            )
            _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
            _cleanup_committed(parent_fd, record, snapshot)
            return
        _authenticate_recovery_inputs(parent_fd, json_member, markdown_member)
        if state == "PREPARED":
            _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
            _rollback(parent_fd, (json_member, markdown_member), exchange)
            _remove_snapshot(parent_fd, _JOURNAL, snapshot)
            return
        if state == "JSON_REPLACED":
            if _desired(parent_fd, json_member) and _desired(parent_fd, markdown_member):
                _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
                _commit_recovered(parent_fd, record, exchange)
                return
            if _desired(parent_fd, json_member) and _prior_or_absent(parent_fd, markdown_member):
                _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
                _install_member(parent_fd, markdown_member, exchange)
                record, _ = _transition(parent_fd, record, "PAIR_REPLACED", exchange)
                _commit_recovered(parent_fd, record, exchange)
                return
            _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
            _rollback(parent_fd, (json_member, markdown_member), exchange)
            _remove_snapshot(parent_fd, _JOURNAL, snapshot)
            return
        if state == "PAIR_REPLACED":
            if _desired(parent_fd, json_member) and _desired(parent_fd, markdown_member):
                _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
                _commit_recovered(parent_fd, record, exchange)
                return
            _require_same_snapshot_fs(parent_fd, _JOURNAL, snapshot, max_bytes=_JOURNAL_MAX_BYTES)
            _rollback(parent_fd, (json_member, markdown_member), exchange)
            _remove_snapshot(parent_fd, _JOURNAL, snapshot)
            return
    except (KeyError, TypeError, ValueError, OSError, ConfinedFileError) as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc


def _commit_recovered(parent_fd: int, record: dict[str, object], exchange: AtomicExchange) -> None:
    json_member, markdown_member = member(record["json"]), member(record["markdown"])
    _require_desired(parent_fd, json_member)
    _require_desired(parent_fd, markdown_member)
    _publish_identity(parent_fd, _IDENTITY, cast(dict[str, object], record["identity"]), max_bytes=_JOURNAL_MAX_BYTES)
    record, journal = _transition(parent_fd, record, "COMMITTED", exchange)
    _cleanup(parent_fd, record, journal)


def _prepare_member(parent_fd: int, name: str, content: bytes, token: str) -> PairMember:
    staged = f".{name}.dpone-stage-{token}"
    _write_new(parent_fd, staged, content)
    staged_snapshot = _require(parent_fd, staged, digest(content))
    prior = _optional(parent_fd, name)
    if prior is None:
        return PairMember(
            name, digest(content), staged, staged_snapshot.identity.device, staged_snapshot.identity.inode, None, None
        )
    backup = f".{name}.dpone-backup-{token}"
    try:
        os.link(name, backup, src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
        os.fsync(parent_fd)
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
    _require(parent_fd, backup, prior.sha256)
    return PairMember(
        name,
        digest(content),
        staged,
        staged_snapshot.identity.device,
        staged_snapshot.identity.inode,
        backup,
        prior.sha256,
    )


def _install_member(parent_fd: int, item: PairMember, exchange: AtomicExchange) -> None:
    _require(parent_fd, item.staged, item.digest)
    if item.backup is None:
        try:
            os.link(item.staged, item.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
            os.fsync(parent_fd)
        except FileExistsError as exc:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
        except OSError as exc:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
        _require_desired(parent_fd, item)
        return
    _require_backup(parent_fd, item)
    current = _require(parent_fd, item.name, cast(str, item.backup_digest))
    backup = _require(parent_fd, cast(str, item.backup), cast(str, item.backup_digest))
    _same_inode(current, backup)
    try:
        exchange(parent_fd, item.name, item.staged)
        os.fsync(parent_fd)
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
    _require_desired(parent_fd, item)
    _require(parent_fd, item.staged, cast(str, item.backup_digest))


def _rollback(parent_fd: int, items: tuple[PairMember, PairMember], exchange: AtomicExchange) -> None:
    for item in items:
        _require_staged_and_backup(parent_fd, item)
        current = _optional(parent_fd, item.name)
        if item.backup is None:
            if current is not None:
                staged = _require(parent_fd, item.staged, item.digest)
                _same_inode(current, staged)
                _remove_snapshot(parent_fd, item.name, current)
        elif current is not None and current.sha256 == item.digest:
            _require_desired_owned(item, current)
            try:
                exchange(parent_fd, item.name, item.staged)
                os.fsync(parent_fd)
            except OSError as exc:
                raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
            _require(parent_fd, item.name, cast(str, item.backup_digest))
        elif current is None or current.sha256 != item.backup_digest:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
        elif item.backup is not None:
            _same_inode(current, _require(parent_fd, item.backup, cast(str, item.backup_digest)))
        cleanup_staged = _optional(parent_fd, item.staged)
        if cleanup_staged is None:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
        _remove_snapshot(parent_fd, item.staged, cleanup_staged)
        if item.backup is not None:
            _remove_snapshot(parent_fd, item.backup, _require(parent_fd, item.backup, cast(str, item.backup_digest)))


def _cleanup(parent_fd: int, record: dict[str, object], journal: ConfinedFileSnapshot) -> None:
    _require_same_snapshot_fs(parent_fd, _JOURNAL, journal, max_bytes=_JOURNAL_MAX_BYTES)
    for item in (member(record["json"]), member(record["markdown"])):
        _require_staged_and_backup(parent_fd, item)
        staged = _optional(parent_fd, item.staged)
        if staged is None:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
        _remove_snapshot(parent_fd, item.staged, staged)
        if item.backup is not None:
            _remove_snapshot(parent_fd, item.backup, _require(parent_fd, item.backup, cast(str, item.backup_digest)))
    _remove_snapshot(parent_fd, _JOURNAL, journal)


def _cleanup_committed(parent_fd: int, record: dict[str, object], journal: ConfinedFileSnapshot) -> None:
    _require_same_snapshot_fs(parent_fd, _JOURNAL, journal, max_bytes=_JOURNAL_MAX_BYTES)
    for item in (member(record["json"]), member(record["markdown"])):
        staged = _optional(parent_fd, item.staged)
        if staged is not None:
            allowed = {item.digest, item.backup_digest}
            if staged.sha256 not in allowed:
                raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
            _remove_snapshot(parent_fd, item.staged, staged)
        if item.backup is not None:
            backup = _optional(parent_fd, item.backup)
            if backup is not None:
                _require_backup(parent_fd, item)
                _remove_snapshot(parent_fd, item.backup, backup)
    _remove_snapshot(parent_fd, _JOURNAL, journal)


def _create_journal(parent_fd: int, record: dict[str, object]) -> ConfinedFileSnapshot:
    _write_new(parent_fd, _JOURNAL, encode(record))
    return _require(parent_fd, _JOURNAL, digest(encode(record)), max_bytes=_JOURNAL_MAX_BYTES)


def _transition(
    parent_fd: int, record: dict[str, object], state: str, exchange: AtomicExchange
) -> tuple[dict[str, object], ConfinedFileSnapshot]:
    current = _require(parent_fd, _JOURNAL, digest(encode(record)), max_bytes=_JOURNAL_MAX_BYTES)
    next_record = record | {"state": state}
    staged = f".{_JOURNAL}.next-{secrets.token_hex(16)}"
    _write_new(parent_fd, staged, encode(next_record))
    result = _replace_if_current_fs(
        parent_fd,
        _JOURNAL,
        staged,
        expected_digest=current.sha256,
        max_bytes=_JOURNAL_MAX_BYTES,
        atomic_exchange=exchange,
    )
    if not result.committed or result.cleanup_required:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    return next_record, _require(parent_fd, _JOURNAL, digest(encode(next_record)), max_bytes=_JOURNAL_MAX_BYTES)


def _recover_journal_transition(parent_fd: int, exchange: AtomicExchange) -> None:
    _recover_transaction_fs(parent_fd, _JOURNAL, max_bytes=_JOURNAL_MAX_BYTES, atomic_exchange=exchange)


def _authenticate_recovery_inputs(parent_fd: int, *items: PairMember) -> None:
    for item in items:
        _require_staged_and_backup(parent_fd, item)
        current = _optional(parent_fd, item.name)
        if item.backup is None:
            if current is not None:
                _require_desired_owned(item, current)
            continue
        backup = _require(parent_fd, cast(str, item.backup), cast(str, item.backup_digest))
        if current is None:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
        if current.sha256 == item.digest:
            _require_desired_owned(item, current)
        elif current.sha256 == item.backup_digest:
            _same_inode(current, backup)
        else:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def _require_staged_and_backup(parent_fd: int, item: PairMember) -> None:
    staged = _optional(parent_fd, item.staged)
    allowed_digests = {item.digest}
    if item.backup_digest is not None:
        allowed_digests.add(item.backup_digest)
    if staged is None or staged.sha256 not in allowed_digests:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    if item.backup is not None:
        _require_backup(parent_fd, item)


def _require_backup(parent_fd: int, item: PairMember) -> None:
    if item.backup is None or item.backup_digest is None:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    _require(parent_fd, item.backup, item.backup_digest)


def _desired(parent_fd: int, item: PairMember) -> bool:
    return _desired_owned_fs(parent_fd, item.name, item.digest, item.desired_device, item.desired_inode)


def _prior_or_absent(parent_fd: int, item: PairMember) -> bool:
    return _prior_or_absent_fs(parent_fd, item.name, item.backup_digest)


def _require_desired(parent_fd: int, item: PairMember) -> None:
    _require_desired_owned(item, _require(parent_fd, item.name, item.digest))


def _require_desired_owned(item: PairMember, snapshot: ConfinedFileSnapshot) -> None:
    if snapshot.identity.device != item.desired_device or snapshot.identity.inode != item.desired_inode:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def _validate_inputs(*args: object) -> None:
    try:
        validate_inputs(*cast(tuple[str, str, bytes, bytes, str, str], args), max_bytes=_MAX_BYTES)
    except ValueError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_INVALID") from exc


def _exchange_or_raise() -> AtomicExchange:
    try:
        return get_native_atomic_exchange()
    except AtomicExchangeUnsupported as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNSUPPORTED") from exc


def _notify(hook: Callable[[str], None] | None, phase: str) -> None:
    if hook is not None:
        hook(phase)
