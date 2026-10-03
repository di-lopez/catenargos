WITH rebased AS (
    SELECT
        date,
        index_type,
        (value_usd_infadj / FIRST(value_usd_infadj) OVER (PARTITION BY index_type ORDER BY date)) * 100 AS value
    FROM {{ ref("icc_infadj") }}
)

SELECT
    date,
    index_type,
    value,
    avg(value) OVER rolling_6m_window AS value_avg_6m,
    min(value) OVER rolling_6m_window AS value_min_6m,
    max(value) OVER rolling_6m_window AS value_max_6m
FROM rebased
WINDOW rolling_6m_window AS (
    PARTITION BY index_type
    ORDER BY date
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
)
ORDER BY date
