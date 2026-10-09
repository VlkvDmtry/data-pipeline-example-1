"""Тесты разбиения периода на месяцы и диапазонов дат."""

from datetime import date

import pytest

from p1.dates import date_range, month_chunks, parse_date


def test_month_chunks_splits_by_calendar_month():
    # Период захватывает конец января, весь февраль и начало марта
    chunks = month_chunks(date(2025, 1, 15), date(2025, 3, 10))
    assert chunks == [
        (date(2025, 1, 15), date(2025, 1, 31)),
        (date(2025, 2, 1), date(2025, 2, 28)),
        (date(2025, 3, 1), date(2025, 3, 10)),
    ]


def test_month_chunks_single_day():
    # Ежедневный запуск: один кусок из одного дня
    assert month_chunks(date(2026, 10, 8), date(2026, 10, 8)) == [
        (date(2026, 10, 8), date(2026, 10, 8))
    ]


def test_month_chunks_leap_year_and_year_boundary():
    chunks = month_chunks(date(2023, 12, 1), date(2024, 2, 29))
    assert chunks[-1] == (date(2024, 2, 1), date(2024, 2, 29))
    assert len(chunks) == 3


def test_ranges_reject_reversed_period():
    with pytest.raises(ValueError):
        month_chunks(date(2025, 2, 1), date(2025, 1, 1))
    with pytest.raises(ValueError):
        date_range(date(2025, 2, 1), date(2025, 1, 1))


def test_date_range_inclusive():
    days = date_range(date(2025, 1, 30), date(2025, 2, 2))
    assert days == [date(2025, 1, 30), date(2025, 1, 31), date(2025, 2, 1), date(2025, 2, 2)]


def test_parse_date():
    assert parse_date("2025-01-15") == date(2025, 1, 15)
    assert parse_date(date(2025, 1, 15)) == date(2025, 1, 15)
