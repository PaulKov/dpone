from __future__ import annotations

import threading
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone._compat import UTC
from dpone.runtime.connectors.api.cbr import CbrConnector
from dpone.runtime.support.cbr_xml_parser import deduplicate_rows, parse_xml_daily, resolve_date_option

SAMPLE_XML = """<?xml version=\"1.0\" encoding=\"windows-1251\"?>
<ValCurs Date=\"10.03.2026\" name=\"Foreign Currency Market\">
  <Valute ID=\"R01010\">
    <NumCode>036</NumCode>
    <CharCode>AUD</CharCode>
    <Nominal>1</Nominal>
    <Name>Австралийский доллар</Name>
    <Value>59,1234</Value>
  </Valute>
  <Valute ID=\"R01235\">
    <NumCode>840</NumCode>
    <CharCode>USD</CharCode>
    <Nominal>1</Nominal>
    <Name>Доллар США</Name>
    <Value>92,5000</Value>
  </Valute>
</ValCurs>
"""


def test_cbr_resolve_date_option_supports_iso_and_today() -> None:
    assert resolve_date_option("2026-03-10") == date(2026, 3, 10)
    assert resolve_date_option(date(2026, 3, 10)) == date(2026, 3, 10)
    assert resolve_date_option(datetime(2026, 3, 10, 8, 0, 0)) == date(2026, 3, 10)
    assert isinstance(resolve_date_option("today"), date)


def test_cbr_parse_xml_daily_and_deduplicate_rows() -> None:
    rows = parse_xml_daily(SAMPLE_XML, loaded_at_utc=datetime(2026, 3, 10, 12, 0, tzinfo=UTC))
    assert len(rows) == 2
    assert rows[0]["as_of_date"] == "2026-03-10"
    assert rows[0]["char_code"] == "AUD"
    assert rows[0]["nominal"] == 1
    assert rows[0]["value"] == 59.1234
    assert rows[0]["vunit_rate"] == 59.1234
    dup_rows = rows + [dict(rows[0], raw_value="59,1234")]
    assert len(deduplicate_rows(dup_rows)) == 2


@pytest.fixture()
def cbr_mock_server():
    requests_seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            requests_seen.append(self.path)
            parsed = urlparse(self.path)
            assert parsed.path.endswith("/XML_daily.asp")
            params = parse_qs(parsed.query)
            body = SAMPLE_XML
            if "date_req" in params and params["date_req"] == ["09/03/2026"]:
                body = SAMPLE_XML.replace("10.03.2026", "09.03.2026")
            self.send_response(200)
            self.send_header("Content-Type", "application/xml; charset=windows-1251")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def log_message(self, format, *args):  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, requests_seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_cbr_connector_get_xml_daily_and_get_resources(cbr_mock_server) -> None:
    server, requests_seen = cbr_mock_server
    connector = CbrConnector(base_url=f"http://127.0.0.1:{server.server_port}", retries=1)
    xml = connector.get_xml_daily(date(2026, 3, 10))
    assert "ValCurs" in xml
    rows = list(connector.get_resources("xml_daily_asp", {"start_date": "2026-03-09", "end_date": "2026-03-10"}))
    assert len(rows) == 4
    assert rows[0]["char_code"] == "AUD"
    assert any("date_req=09%2F03%2F2026" in path for path in requests_seen)


def test_cbr_connector_health_check(cbr_mock_server) -> None:
    server, _ = cbr_mock_server
    connector = CbrConnector(base_url=f"http://127.0.0.1:{server.server_port}", retries=1)
    assert connector.health_check() is True
