"""Утилиты для дат: разбор строк, диапазоны дней, разбиение периода по месяцам."""

from __future__ import annotations

from datetime import date, timedelta


def parse_date(value: str | date) -> date:
    """Превращает 'YYYY-MM-DD' в date (date возвращается как есть)."""
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def date_range(start: date, end: date) -> list[date]:
    """Все дни от start до end включительно."""
    if start > end:
        raise ValueError(f"start {start} позже end {end}")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Разбивает период [start, end] на куски по календарным месяцам.

    Пример: 2025-01-15..2025-03-10 ->
        (2025-01-15, 2025-01-31), (2025-02-01, 2025-02-28), (2025-03-01, 2025-03-10)
    Зачем: один запрос к API = один город за один месяц. Это укладывается
    в лимиты Open-Meteo даже при backfill за несколько лет.
    """
    if start > end:
        raise ValueError(f"start {start} позже end {end}")
    chunks = []
    cursor = start
    while cursor <= end:
        # Первое число следующего месяца
        next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        # Конец куска: последний день месяца или end, что раньше
        chunk_end = min(next_month - timedelta(days=1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks
