"""All authoring schema families reject non-boolean replay selection."""

import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "name", ["etl-config", "etl-batch-manifest", "etl-flow-manifest", "etl-flow-fragment-manifest"]
)
def test_sink_options_replay_schema_is_strict(name):
    schema = json.loads((ROOT / "src/dpone/schema" / f"{name}.schema.json").read_text())
    if name == "etl-config":
        sink = schema["properties"]["sink"]
    elif name == "etl-batch-manifest":
        sink = schema["definitions"]["process_fragment"]["properties"]["sink"]
    else:
        sink = schema["definitions"]["process"]["properties"]["sink"]["allOf"][1]
    options = sink["properties"]["options"]
    validator = Draft7Validator(options)
    for value in [True, False]:
        assert not list(validator.iter_errors({"durable_quality_replay": value}))
    for value in [None, 0, 1, "true", "false", [], {}]:
        assert list(validator.iter_errors({"durable_quality_replay": value}))
