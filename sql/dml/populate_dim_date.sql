-- Sentinel Claims Platform — Populate Date Dimension


INSERT INTO warehouse.dim_date
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INTEGER                                    AS date_key,
    datum                                                                   AS full_date,
    EXTRACT(YEAR FROM datum)                                                AS year,
    EXTRACT(QUARTER FROM datum)                                             AS quarter,
    EXTRACT(MONTH FROM datum)                                               AS month,
    TO_CHAR(datum, 'Month')                                                 AS month_name,
    EXTRACT(DAY FROM datum)                                                 AS day_of_month,
    EXTRACT(DOW FROM datum)                                                 AS day_of_week,
    TO_CHAR(datum, 'Day')                                                   AS day_name,
    CASE WHEN EXTRACT(DOW FROM datum) IN (0,6) THEN TRUE ELSE FALSE END     AS is_weekend,
    CASE WHEN datum = LAST_DAY(datum)          THEN TRUE ELSE FALSE END     AS is_month_end
FROM (
    SELECT ('2020-01-01'::DATE + (a.n + b.n*10 + c.n*100 + d.n*1000)) AS datum
    FROM
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c,
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) d
    WHERE (a.n + b.n*10 + c.n*100 + d.n*1000) <= 4017
) dates
WHERE datum <= '2030-12-31';
