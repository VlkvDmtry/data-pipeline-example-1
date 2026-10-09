"""DAG weather_backfill: загрузка истории за произвольный период.

Запускается вручную с параметрами start_date и end_date (YYYY-MM-DD):
  - в UI: Trigger DAG, поля формы
  - из терминала: .\\scripts\\p1.ps1 backfill 2025-01-01 2025-12-31
Период режется на месяцы: 1 запрос к API = 1 город × 1 месяц.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from airflow.sdk import Param, dag, get_current_context, task

from p1_common import DEFAULT_ARGS, build_pipeline

# Historical Forecast API хранит данные с этой даты
API_MIN_DATE = date(2022, 1, 1)


@dag(
    dag_id="weather_backfill",
    # Без расписания: только ручной запуск
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    # Параметры запуска: Airflow проверит формат и покажет их формой в UI
    params={
        "start_date": Param("2026-09-01", type="string", format="date", description="Начало периода"),
        "end_date": Param("2026-09-30", type="string", format="date", description="Конец периода (включительно)"),
    },
    tags=["p1", "weather"],
    doc_md=__doc__,
)
def weather_backfill():
    @task(multiple_outputs=True)
    def resolve_dates() -> dict:
        """Берёт период из параметров запуска и проверяет его."""
        params = get_current_context()["params"]
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"])
        today = datetime.now(timezone.utc).date()
        if start > end:
            raise ValueError(f"start_date {start} позже end_date {end}")
        if start < API_MIN_DATE:
            raise ValueError(f"API хранит данные только с {API_MIN_DATE}")
        if end >= today:
            raise ValueError(f"end_date должен быть раньше сегодняшнего дня ({today})")
        return {"start": start.isoformat(), "end": end.isoformat()}

    dates = resolve_dates()
    build_pipeline(dates["start"], dates["end"])


weather_backfill()
