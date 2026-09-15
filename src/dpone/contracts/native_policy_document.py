"""Pure complete policy-member identity checks before original-store construction."""

from __future__ import annotations

from hashlib import sha256

from dpone.contracts.dbt_project_bundle import DbtProjectBundle
from dpone.contracts.native_delivery import NativePolicyDocumentRef
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json


def verify_native_policy_member(
    payload: bytes,
    descriptor: NativePolicyDocumentRef,
    bundle: DbtProjectBundle,
    *,
    max_bytes: int,
) -> None:
    """Require exact bounded canonical bytes and one matching project member.

    This is not v4 schema validation, archive authentication or qualification.
    The original verifier must authenticate the archive first and apply the full
    closed v4 policy contract before any workspace credential acquisition.
    """
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("policy read bound must be an exact positive integer")
    if type(descriptor) is not NativePolicyDocumentRef or type(bundle) is not DbtProjectBundle:
        raise ValueError("policy verification requires exact descriptor and bundle contracts")
    descriptor.__post_init__()
    bundle.__post_init__()
    if type(payload) is not bytes or len(payload) > max_bytes or len(payload) != descriptor.bytes:
        raise ValueError("policy bytes differ from the declared size or admitted bound")
    members = tuple(member for member in bundle.files if member.path == descriptor.path)
    if len(members) != 1 or (members[0].sha256, members[0].bytes) != (descriptor.sha256, descriptor.bytes):
        raise ValueError("policy descriptor is not the exact unique project member")
    if "sha256:" + sha256(payload).hexdigest() != descriptor.sha256:
        raise ValueError("policy digest differs from complete member bytes")
    if encode_native_delivery_json(decode_native_delivery_json(payload)) != payload:
        raise ValueError("policy member must contain canonical bounded JSON")
