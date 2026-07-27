import time

import pytest
import requests
import responses

from lestrade_edgar.client import EdgarClient
from lestrade_edgar.exceptions import EdgarRequestError


@responses.activate
def test_retries_503_then_200():
    url = "https://www.sec.gov/test"
    responses.add(responses.GET, url, status=503, body=b"no")
    responses.add(responses.GET, url, status=200, body=b"ok")

    c = EdgarClient(
        app_name="LestradeTest",
        contact_email="test@example.com",
        min_interval_s=0.0,
        max_attempts=5,
        base_backoff_s=0.0,
        max_backoff_s=0.0,
    )
    body, status = c.get_bytes(url)
    assert status == 200
    assert body == b"ok"
    assert len(responses.calls) == 2


@responses.activate
def test_no_retry_on_404():
    url = "https://www.sec.gov/missing"
    responses.add(responses.GET, url, status=404, body=b"nope")

    c = EdgarClient(
        app_name="LestradeTest",
        contact_email="test@example.com",
        min_interval_s=0.0,
        base_backoff_s=0.0,
        max_backoff_s=0.0,
    )
    body, status = c.get_bytes(url)
    assert status == 404
    assert len(responses.calls) == 1


@responses.activate
def test_user_agent_override():
    url = "https://data.sec.gov/submissions/CIK0000000000.json"
    responses.add(responses.GET, url, json={"cik": "0", "filings": {"recent": {}}}, status=200)

    c = EdgarClient(
        app_name="MyApp",
        contact_email="ops@example.com",
        user_agent="CustomUA/1.0 (ops@example.com)",
        min_interval_s=0.0,
        base_backoff_s=0.0,
        max_backoff_s=0.0,
    )
    c.get_json(url)
    assert responses.calls[0].request.headers["User-Agent"] == "CustomUA/1.0 (ops@example.com)"
    assert responses.calls[0].request.headers["From"] == "ops@example.com"
    assert responses.calls[0].request.headers["Referer"] == "https://www.sec.gov/"


@responses.activate
def test_user_agent_header():
    url = "https://data.sec.gov/submissions/CIK0000000000.json"
    responses.add(responses.GET, url, json={"cik": "0", "filings": {"recent": {}}}, status=200)

    c = EdgarClient(
        app_name="MyApp",
        contact_email="ops@example.com",
        min_interval_s=0.0,
        base_backoff_s=0.0,
        max_backoff_s=0.0,
    )
    c.get_json(url)
    assert (
        responses.calls[0].request.headers["User-Agent"]
        == "Mozilla/5.0 (MyApp ops@example.com)"
    )
    assert responses.calls[0].request.headers["From"] == "ops@example.com"
    assert responses.calls[0].request.headers["Referer"] == "https://www.sec.gov/"


def test_min_interval_spacing():
    url = "https://www.sec.gov/ping"
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, status=200, body=b"1")
        rsps.add(responses.GET, url, status=200, body=b"2")

        c = EdgarClient(
            app_name="LestradeTest",
            contact_email="test@example.com",
            min_interval_s=0.15,
            base_backoff_s=0.0,
            max_backoff_s=0.0,
        )
        t0 = time.monotonic()
        c.get_bytes(url)
        c.get_bytes(url)
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.12


@responses.activate
def test_connection_error_retries_then_raises():
    url = "https://www.sec.gov/flaky"

    def boom(_):
        raise requests.ConnectionError("reset")

    responses.add_callback(responses.GET, url, callback=boom)
    responses.add_callback(responses.GET, url, callback=boom)

    c = EdgarClient(
        app_name="LestradeTest",
        contact_email="test@example.com",
        min_interval_s=0.0,
        max_attempts=2,
        base_backoff_s=0.0,
        max_backoff_s=0.0,
    )
    with pytest.raises(EdgarRequestError):
        c.get_bytes(url)
