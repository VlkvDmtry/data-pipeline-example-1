-- Обзор витрины: сколько данных загружено и за какой период.
-- Запуск: .\scripts\p1.ps1 chfile clickhouse\queries\01_overview.sql

-- Общий объём: строки, города, первый и последний день.
-- FINAL схлопывает дубли ReplacingMergeTree, которые ещё не слились в фоне.
SELECT
    count()          AS rows,
    uniqExact(city)  AS cities,
    min(dt)          AS first_day,
    max(dt)          AS last_day
FROM weather.daily_city FINAL;

-- Как таблица лежит на диске: партиции (месяцы), строки и сжатие.
-- system.parts - системная таблица с кусками (parts) MergeTree-таблиц.
SELECT
    partition,
    sum(rows)                                        AS rows,
    count()                                          AS parts,  -- сколько кусков ещё не слилось
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
FROM system.parts
WHERE database = 'weather' AND table = 'daily_city' AND active
GROUP BY partition
ORDER BY partition DESC
LIMIT 12;
