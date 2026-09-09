from pathlib import Path

from dpone.runtime.credentials.providers import EnvironmentCredentialsProvider


def test_no_vault_s3_example_matches_environment_provider(
    monkeypatch,
) -> None:
    prefix = "DPONE_CONN_S3_DPONE_ARTIFACTS_WRITER_"
    values = {
        "USERNAME": "example-access-key",
        "PASSWORD": "example-secret-key",
        "ENDPOINT": "https://storage.example",
        "TOKEN": "example-session-token",
        "ADDITIONAL_REGION": "region-1",
    }
    for suffix, value in values.items():
        monkeypatch.setenv(prefix + suffix, value)

    credentials = EnvironmentCredentialsProvider().get_credentials("s3_dpone_artifacts_writer")
    docs = (Path(__file__).parents[1] / "docs/airflow-artifact-trust.md").read_text(encoding="utf-8")

    assert credentials.username == values["USERNAME"]
    assert credentials.password == values["PASSWORD"]
    assert credentials.endpoint == values["ENDPOINT"]
    assert credentials.token == values["TOKEN"]
    assert credentials.additional_params == {"region": values["ADDITIONAL_REGION"]}
    assert prefix + "ADDITIONAL_REGION" in docs
