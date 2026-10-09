-- Помесячная динамика средней температуры: город × месяц.
-- Запуск: .\scripts\p1.ps1 chfile clickhouse\queries\03_monthly_trends.sql

-- Средняя температура по месяцам для нескольких городов.
-- toStartOfMonth округляет дату до первого числа месяца.
SELECT
    toStartOfMonth(dt)                               AS month,
    round(avgIf(temp_avg, city = 'moscow'), 1)       AS moscow,
    round(avgIf(temp_avg, city = 'yakutsk'), 1)      AS yakutsk,
    round(avgIf(temp_avg, city = 'sochi'), 1)        AS sochi,
    round(avgIf(temp_avg, city = 'london'), 1)       AS london,
    round(avgIf(temp_avg, city = 'dubai'), 1)        AS dubai
FROM weather.daily_city FINAL
GROUP BY month
ORDER BY month;

-- Сравнение год к году: одинаковые месяцы разных лет для Москвы.
-- Оконная функция lagInFrame берёт значение из предыдущей строки окна (тот же месяц прошлого года).
-- toNullable: у первой строки окна предыдущей нет, и вместо 0 получится NULL.
SELECT
    toMonth(dt)                     AS month_num,
    toYear(dt)                      AS year,
    round(avg(temp_avg), 1)         AS avg_temp,
    round(avg_temp - lagInFrame(toNullable(avg_temp)) OVER (
        PARTITION BY month_num ORDER BY year
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ), 1)                           AS diff_vs_prev_year
FROM weather.daily_city FINAL
WHERE city = 'moscow'
GROUP BY month_num, year
ORDER BY month_num, year;
