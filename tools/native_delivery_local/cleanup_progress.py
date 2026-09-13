"""Fenced intent and acknowledgement for retrying a partially completed cleanup."""

from __future__ import annotations

import json

from .inventory import canonical


def drop_owned(store, key, expected, read_identity, drop, lease):
    """Absence is acceptable only after our durable deletion intent.

    A replacement at the same name is rejected even after a prior acknowledgement.
    The caller validates database/namespace authority before invoking this helper.
    """
    store.assert_lease(lease)
    prior = store.load(key)
    if prior is not None:
        progress = json.loads(prior.payload)
        if progress.get("identity") != expected or progress.get("phase") not in {"intent", "absent"}:
            raise ValueError("local_fixture.cleanup_identity_changed")
    actual = read_identity()
    if actual is not None and actual != expected:
        raise ValueError("local_fixture.cleanup_object_replaced")
    if prior is None:
        if actual is None:
            raise ValueError("local_fixture.cleanup_unproven_absence")
        prior = store.save(key, None, canonical({"identity": expected, "phase": "intent"}).decode(), lease)
    if actual is not None:
        store.assert_lease(lease)
        drop()
        if read_identity() is not None:
            raise RuntimeError("local_fixture.cleanup_not_absent")
    store.assert_lease(lease)
    store.save(key, prior.revision, canonical({"identity": expected, "phase": "absent"}).decode(), lease)
