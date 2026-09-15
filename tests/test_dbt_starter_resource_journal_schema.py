"""Closed codec rejects malformed evidence independently of journal I/O."""

import pytest
from tools.dbt_self_service.starter_resource_journal_schema import RESOURCE_PATHS, read_events, validate_event


@pytest.mark.parametrize("path", ["../foreign", "/foreign", "src/../foreign", "src\\foreign", "src/foreign"])
def test_event_rejects_paths_outside_inventory(path):
    with pytest.raises(ValueError):
        validate_event({"phase": "APPLYING", "path": path})


@pytest.mark.parametrize(
    "content",
    [
        b'{"phase":"PREPARED","sequence":0}',
        b'{"phase":"PREPARED","sequence":1}\n',
        b'{"phase":"PREPARED","sequence":0,"sequence":0}\n',
        b'{"phase":"PREPARED","sequence":true}\n',
    ],
)
def test_event_decoder_rejects_truncation_or_invalid_sequence(content):
    with pytest.raises(ValueError):
        read_events(content)


def test_fixed_target_pending_record_is_valid():
    validate_event({"phase": "APPLYING", "path": RESOURCE_PATHS[0]})
    assert read_events(b'{"phase":"PREPARING","sequence":0}\n') == [{"phase": "PREPARING"}]
