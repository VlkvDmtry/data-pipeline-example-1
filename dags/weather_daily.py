"""DAG weather_daily: ежедневная загрузка погоды за вчерашний день (UTC)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

from p1_common import DEFAULT_ARGS, build_pipeline


@dag(
    dag_id="weather_daily",
    # Каждый день в 03:00 UTC: к этому времени API уже отдаёт данные за вчера целиком
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    # Не догонять пропущенные запуски с start_date (для истории есть weather_backfill)
    catchup=False,
    # Не больше одного запуска одновременно: Spark и ClickHouse обрабатывают по одному
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["p1", "weather"],
    doc_md=__doc__,
)
def weather_daily():
    @task(multiple_outputs=True)
    def resolve_dates() -> dict:
        """Вычисляет целевой день: logical_date минус 1 день.

        multiple_outputs=True кладёт ключи dict в отдельные XCom (start, end),
        поэтому ниже можно писать dates["start"].
        """
        ctx = get_current_context()
        # У ручного запуска logical_date может не быть, тогда берём время запуска run_after
        ref = ctx.get("logical_date") or ctx["dag_run"].run_after
        day = (ref - timedelta(days=1)).date()
        return {"start": day.isoformat(), "end": day.isoformat()}

    dates = resolve_dates()
    build_pipeline(dates["start"], dates["end"])


weather_daily()
