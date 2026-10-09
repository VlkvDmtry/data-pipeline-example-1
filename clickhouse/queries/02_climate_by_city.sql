-- Климат городов за весь период: средние, экстремумы, осадки.
-- Запуск: .\scripts\p1.ps1 chfile clickhouse\queries\02_climate_by_city.sql

SELECT
    city_name,
    country,
    round(avg(temp_avg), 1)                  AS avg_temp,      -- средняя температура
    min(temp_min)                            AS abs_min,       -- абсолютный минимум
    max(temp_max)                            AS abs_max,       -- абсолютный максимум
    -- argMin/argMax возвращают значение одной колонки в строке, где другая минимальна/максимальна
    argMin(dt, temp_min)                     AS coldest_day,
    argMax(dt, temp_max)                     AS hottest_day,
    -- осадки за год: сумма за период, делённая на число лет
    round(sum(precip_sum) / (uniqExact(dt) / 365.25), 0) AS precip_per_year_mm,
    countIf(precip_sum >= 1)                 AS rainy_days     -- дни с осадками от 1 мм
FROM weather.daily_city FINAL
GROUP BY city_name, country
ORDER BY avg_temp DESC;
