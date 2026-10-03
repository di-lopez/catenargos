SELECT
    date,
    index_type,
    (value_usd_infadj / FIRST(value_usd_infadj) OVER (PARTITION BY index_type ORDER BY date)) * 100 AS value
FROM {{ ref("icc_infadj") }}
ORDER BY date
