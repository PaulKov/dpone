"""Frozen old bytes, independent of current writers and their random grants.

This guards preservation inputs. It does not certify the future historical
reader, real transport completion, live-WAL handling or publication recovery.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from dpone.adapters.clickhouse_publication_codec import PublicationRecordCodecError, decode_record, encode_record

FIXTURES = Path(__file__).parent / "fixtures" / "clickhouse_prototype_history_20261001" / "frozen"
SOURCE_COMMIT = "5a36601e26fb826ed4ea1b4a08aca722d366ffec"


def _read_json(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _hashes() -> dict[str, str]:
    return {
        path.relative_to(FIXTURES).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(FIXTURES.rglob("*"))
        if path.is_file()
    }


def test_frozen_bundle_has_complete_provenance_and_no_unlisted_files():
    provenance = _read_json("provenance.json")
    assert provenance["source_commit"] == SOURCE_COMMIT
    assert provenance["live_certificate"] is False
    assert provenance["kind"] == "synthetic_pre_consolidation_fixtures"
    assert len(provenance["producer_sources"]) == 35
    assert {"src/dpone/__init__.py", "tests/__init__.py"} <= provenance["producer_sources"].keys()
    actual = _hashes()
    assert actual.pop("provenance.json")
    assert actual == provenance["files_sha256"]
    assert len(actual) == 14
    assert actual["producer.py.txt"] == provenance["producer_sha256"]


@pytest.mark.parametrize("index", range(24))
def test_original_record_bytes_and_selector_survive_current_reader(index):
    vector = _read_json("publication-records.json")[index]
    encoded = vector["encoded"]
    assert hashlib.sha256(encoded.encode()).hexdigest() == vector["sha256"]
    record = decode_record(encoded)
    assert record.intent.method == vector["method"]
    assert record.state.value == vector["state"]
    assert record.claim_granted is vector["claim_granted"]
    assert record.schema_version == "dpone.clickhouse.guarded-publication.v2"
    assert encode_record(record) == encoded


def test_frozen_vectors_cover_methods_and_distinct_claim_history():
    combinations = {
        (value["method"], value["state"], value["claim_granted"]) for value in _read_json("publication-records.json")
    }
    assert combinations == {
        (method, state, claimed)
        for method in ("replace_partition", "exchange", "rename", "noop")
        for state, claimed in (
            ("prepared", False),
            ("claimed", True),
            ("unknown", False),
            ("unknown", True),
            ("committed", True),
            ("not_published", False),
        )
    }


@pytest.mark.parametrize("index", range(24))
@pytest.mark.parametrize("damage", ["version", "extra", "duplicate"])
def test_original_record_corruption_rejects_without_rewriting_fixture(index, damage):
    original = _read_json("publication-records.json")[index]["encoded"]
    payload = json.loads(original)
    if damage == "version":
        payload["schema_version"] = "unknown"
    elif damage == "extra":
        payload["claim_token"] = "not-authority"
    encoded = json.dumps(payload)
    if damage == "duplicate":
        encoded = encoded[:-1] + ',"state":"prepared"}'
    with pytest.raises(PublicationRecordCodecError):
        decode_record(encoded)
    assert _read_json("publication-records.json")[index]["encoded"] == original


@pytest.mark.parametrize("case", ["v1-prepared", "v1-may-have-sent", "v1-terminal", "v2-may-have-sent", "v2-closed"])
def test_quiesced_snapshot_reads_preserve_bytes_and_transport_distinction(case):
    before = _hashes()
    path = FIXTURES / case / "authority.db"
    uri = path.resolve().as_uri() + "?mode=ro&immutable=1"
    for _ in range(2):
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
            assert db.execute("PRAGMA foreign_key_check").fetchall() == []
            assert db.execute("SELECT schema_version,deployment FROM authority_metadata").fetchall() == [
                (f"dpone.clickhouse.authority.{case[:2]}", "deployment")
            ]
            record, transport = db.execute("SELECT record,transport FROM operations").fetchone()
            if case == "v1-terminal":
                assert json.loads(record)["state"] == "claimed"
                assert transport == "closed_terminal"
            if case.startswith("v2"):
                assert db.execute("SELECT count(*) FROM name_reservations").fetchone() == (2,)
                assert db.execute("SELECT count(*) FROM candidate_requests").fetchone() == (2,)
                assert db.execute("SELECT count(*) FROM candidate_seals").fetchone() == (0,)
                diagnostic = _read_json(f"{case}-diagnostics.json")
                assert diagnostic["safe_to_retry"] is False
                assert diagnostic["owner_retained"] is True
                assert diagnostic["sealed"] is False
                assert diagnostic["uncertain_requests"] == (1 if case.endswith("may-have-sent") else 0)
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                db.execute("UPDATE authority_metadata SET deployment='never'")
    assert _hashes() == before
