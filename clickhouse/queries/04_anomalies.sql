-- Температурные аномалии: дни, когда средняя температура сильно отклонилась
-- от «нормы» (средней за 30 предыдущих дней, её считает Spark).
-- Запуск: .\scripts\p1.ps1 chfile clickhouse\queries\04_anomalies.sql

-- Топ-15 самых сильных аномалий (и потеплений, и похолоданий)
SELECT
    dt,
    city_name,
    temp_avg,
    temp_norm_30d,
    temp_anomaly,
    if(temp_anomaly > 0, 'тепло', 'холод') AS kind
FROM weather.daily_city FINAL
WHERE temp_anomaly IS NOT NULL
ORDER BY abs(temp_anomaly) DESC
LIMIT 15;

-- Сколько дней с аномалией больше 5 °C в каждом городе, по годам.
-- Где погода «скачет» сильнее всего?
SELECT
    city_name,
    countIf(abs(temp_anomaly) > 5 AND toYear(dt) = 2024) AS y2024,
    countIf(abs(temp_anomaly) > 5 AND toYear(dt) = 2025) AS y2025,
    countIf(abs(temp_anomaly) > 5)                       AS total,
    round(stddevPop(temp_anomaly), 2)                    AS anomaly_stddev
FROM weather.daily_city FINAL
GROUP BY city_name
ORDER BY total DESC;
