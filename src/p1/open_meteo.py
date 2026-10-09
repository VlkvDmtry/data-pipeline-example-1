"""Extract: запросы к Open-Meteo и сохранение сырых ответов в слой raw."""

from __future__ import annotations

import logging
from datetime import date

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from p1 import s3
from p1.config import HOURLY_VARIABLES, City, Settings, load_cities

log = logging.getLogger(__name__)


class OpenMeteoError(Exception):
    """API ответил ошибкой, которую бессмысленно повторять (например, неверные параметры)."""


class RetryableHTTPError(Exception):
    """Временная ошибка (429 Too Many Requests, 5xx): запрос стоит повторить."""


# Повторяем запрос при сетевых сбоях и временных ошибках API:
# до 5 попыток, пауза растёт экспоненциально (2, 4, 8… сек, но не больше 60).
@retry(
    retry=retry_if_exception_type(
        (requests.ConnectionError, requests.Timeout, RetryableHTTPError)
    ),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_hourly(
    city: City,
    start: date,
    end: date,
    *,
    url: str,
    session: requests.Session | None = None,
) -> dict:
    """Запрашивает почасовую погоду для города за период [start, end]."""
    params = {
        "latitude": city.lat,
        "longitude": city.lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        # Список нужных почасовых переменных через запятую
        "hourly": ",".join(HOURLY_VARIABLES),
        # Время в ответе в UTC, так проще объединять города из разных часовых поясов
        "timezone": "UTC",
    }
    http = session or requests
    resp = http.get(url, params=params, timeout=60)
    # 429 (лимит запросов) и 5xx (сбой на стороне API) отдаём tenacity на повтор
    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableHTTPError(f"{resp.status_code} для {city.slug}: {resp.text[:200]}")
    # Остальные 4xx сразу считаем ошибкой: повтор не поможет
    if resp.status_code >= 400:
        raise OpenMeteoError(f"{resp.status_code} для {city.slug}: {resp.text[:500]}")
    payload = resp.json()
    # Open-Meteo иногда возвращает {"error": true, "reason": "..."}
    if payload.get("error"):
        raise OpenMeteoError(f"{city.slug}: {payload.get('reason')}")
    return payload


def extract_range(
    start: date,
    end: date,
    settings: Settings | None = None,
    cities: list[City] | None = None,
) -> list[str]:
    """Скачивает данные всех городов за период и кладёт сырые JSON в raw.

    Возвращает список записанных ключей S3. Повторный запуск перезаписывает
    те же ключи, поэтому функция идемпотентна.
    """
    settings = settings or Settings.from_env()
    cities = cities or load_cities(settings.cities_file)
    client = s3.get_client(settings)
    keys = []
    # Одна HTTP-сессия на все города: переиспользуем TCP-соединение
    with requests.Session() as session:
        for city in cities:
            payload = fetch_hourly(
                city, start, end, url=settings.open_meteo_url, session=session
            )
            key = s3.raw_key(start, end, city.slug)
            s3.put_json(client, settings.s3_bucket, key, payload)
            hours = len(payload.get("hourly", {}).get("time", []))
            log.info("raw: %s (%d часов)", key, hours)
            keys.append(key)
    return keys
