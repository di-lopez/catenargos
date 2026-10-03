SELECT
    CAST(c.date AS DATE) AS date,
    c.index_type AS index_type,
    c.value AS value_ars,
    c.value / m.value AS value_usd
FROM {{ source("silver", "icc") }} AS c
LEFT JOIN {{ source("argmacro", "usdblue") }} AS m
ON c.date = m.date
ORDER BY c.date