"""Общие фикстуры pytest. Тесты не ходят в сеть и не требуют MinIO/ClickHouse."""

import json
from pathlib import Path

import pytest

from p1.config import City

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_payload() -> dict:
    """Реальный ответ Open-Meteo: Москва, 2026-10-07, 24 часа."""
    return json.loads((FIXTURES / "open_meteo_sample.json").read_text(encoding="utf-8"))


@pytest.fixture
def moscow() -> City:
    return City(slug="moscow", name="Москва", country="RU", lat=55.7558, lon=37.6173)


@pytest.fixture
def london() -> City:
    return City(slug="london", name="Лондон", country="GB", lat=51.5074, lon=-0.1278)
