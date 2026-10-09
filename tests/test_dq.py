"""Тесты DQ-проверок staging и витрины."""

from datetime import date

import pandas as pd

from p1.dq import check_mart_stats, check_staging_frame
from p1.transform import clean, parse_response

DAY = date(2026, 10, 7)


def make_day(sample_payload, *cities) -> pd.DataFrame:
    """Почасовые данные за день по нескольким городам (одинаковые значения, нам это неважно)."""
    return clean(pd.concat([parse_response(sample_payload, c) for c in cities], ignore_index=True))


def test_valid_day_has_no_errors(sample_payload, moscow, london):
    df = make_day(sample_payload, moscow, london)
    assert check_staging_frame(df, {"moscow", "london"}, DAY) == []


def test_empty_day(moscow):
    assert check_staging_frame(pd.DataFrame(), {"moscow"}, DAY) == [f"[{DAY}] нет данных"]


def test_missing_city_and_incomplete_hours(sample_payload, moscow):
    df = make_day(sample_payload, moscow).iloc[:20]  # у Москвы только 20 часов
    errors = check_staging_frame(df, {"moscow", "london"}, DAY)
    assert any("нет городов: ['london']" in e for e in errors)
    assert any("не 24 часа" in e for e in errors)


def test_out_of_range_and_nulls(sample_payload, moscow):
    df = make_day(sample_payload, moscow)
    df.loc[0, "temperature_c"] = 75.0     # нереальная жара
    df.loc[1:5, "humidity_pct"] = None    # 5 из 24 часов = 21% пропусков
    errors = check_staging_frame(df, {"moscow"}, DAY)
    assert any("temperature_c: 1 значений вне" in e for e in errors)
    assert any("humidity_pct: доля пропусков" in e for e in errors)


def test_duplicates_detected(sample_payload, moscow):
    df = make_day(sample_payload, moscow)
    df = pd.concat([df, df.head(2)], ignore_index=True)
    errors = check_staging_frame(df, {"moscow"}, DAY)
    assert any("2 дублей" in e for e in errors)


def good_stats(**overrides) -> dict:
    stats = {"rows": 40, "cities": 20, "days": 2, "incomplete_days": 0, "bad_temp_order": 0, "nan_temp": 0}
    stats.update(overrides)
    return stats


def test_mart_stats_ok():
    assert check_mart_stats(good_stats(), n_cities=20, n_days=2) == []


def test_mart_stats_incomplete():
    errors = check_mart_stats(good_stats(rows=39, cities=19), n_cities=20, n_days=2)
    assert any("строк 39, ожидалось 40" in e for e in errors)
    assert any("городов 19" in e for e in errors)


def test_mart_stats_inconsistent_values():
    errors = check_mart_stats(good_stats(bad_temp_order=3, incomplete_days=1), n_cities=20, n_days=2)
    assert len(errors) == 2
