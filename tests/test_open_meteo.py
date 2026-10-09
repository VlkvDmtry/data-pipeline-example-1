"""Тесты клиента Open-Meteo. Вместо реальной сети используется подставная сессия."""

from datetime import date

import pytest
import requests

from p1 import open_meteo
from p1.open_meteo import OpenMeteoError, RetryableHTTPError, fetch_hourly


class FakeResponse:
    """Минимальная замена requests.Response."""

    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    """Отдаёт заранее заданные ответы по очереди и запоминает параметры запросов."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    """Отключаем паузы tenacity, чтобы тесты на повторы шли мгновенно."""
    monkeypatch.setattr(open_meteo.fetch_hourly.retry, "sleep", lambda _: None)


def test_fetch_sends_expected_params(sample_payload, moscow):
    session = FakeSession([FakeResponse(200, sample_payload)])
    payload = fetch_hourly(moscow, date(2026, 10, 7), date(2026, 10, 7), url="http://x", session=session)
    assert payload == sample_payload
    params = session.calls[0]
    assert params["latitude"] == moscow.lat
    assert params["timezone"] == "UTC"
    assert "temperature_2m" in params["hourly"]


def test_fetch_retries_on_429_and_network_error(sample_payload, moscow):
    # Сначала лимит запросов, потом обрыв сети, потом успех: итого 3 попытки
    session = FakeSession([
        FakeResponse(429, text="Too many requests"),
        requests.ConnectionError("boom"),
        FakeResponse(200, sample_payload),
    ])
    payload = fetch_hourly(moscow, date(2026, 10, 7), date(2026, 10, 7), url="http://x", session=session)
    assert payload == sample_payload
    assert len(session.calls) == 3


def test_fetch_gives_up_after_max_attempts(moscow):
    session = FakeSession([FakeResponse(503)] * 5)
    with pytest.raises(RetryableHTTPError):
        fetch_hourly(moscow, date(2026, 10, 7), date(2026, 10, 7), url="http://x", session=session)
    assert len(session.calls) == 5


def test_fetch_does_not_retry_client_error(moscow):
    # 400 = неверные параметры: повторять бессмысленно, падаем с первой попытки
    session = FakeSession([FakeResponse(400, text="bad request")])
    with pytest.raises(OpenMeteoError):
        fetch_hourly(moscow, date(2026, 10, 7), date(2026, 10, 7), url="http://x", session=session)
    assert len(session.calls) == 1


def test_fetch_raises_on_api_error_payload(moscow):
    session = FakeSession([FakeResponse(200, {"error": True, "reason": "Out of range"})])
    with pytest.raises(OpenMeteoError, match="Out of range"):
        fetch_hourly(moscow, date(2026, 10, 7), date(2026, 10, 7), url="http://x", session=session)
