WITH stats AS (
    SELECT
        date,
        avg(usdm2) as usdm2_avg,
        sum(usdm2)   AS usdm2_sum,
        count(usdm2) AS n,
        min(usdm2) as usdm2_min,
        max(usdm2) as usdm2_max
    FROM {{ ref("cpau_infadj") }}
    WHERE
        model_type NOT IN ('galpon', 'remodelacion baño y cocina')
        AND model_type NOT LIKE 'reforma%'
    GROUP BY date
)

SELECT
    date,
    usdm2_avg,
    usdm2_min,
    usdm2_max,
    sum(usdm2_sum) OVER rolling_6m_window
        / sum(n) OVER rolling_6m_window   AS usdm2_avg_6m,   -- pooled mean
    min(usdm2_min) OVER rolling_6m_window AS usdm2_min_6m,
    max(usdm2_max) OVER rolling_6m_window AS usdm2_max_6m
FROM stats
WINDOW rolling_6m_window AS (
    ORDER BY date
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
)
ORDER BY date