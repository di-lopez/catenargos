WITH joined AS (
SELECT
    date_trunc('month', date) as date,
    trim(lower(model_type)) as model_type,
    arsm2
FROM {{ source("silver", "cpau") }}
UNION ALL
SELECT
    date_trunc('month', date) as date,
    trim(lower(replace(model_type, "_", " "))) as model_type,
    arsm2
FROM {{ ref("cpau_backdata") }}
),

mapped AS (
    SELECT
        j.date,
        m.model_type,
        j.arsm2
    FROM joined AS j
    LEFT JOIN {{ ref("model_type_mapping") }} AS m
    ON j.model_type = m.raw_value
),

-- Clean incorrectly parsed arsm2 that treat thousands separator as decimal point
cleaned AS (
    SELECT
        date,
        model_type,
        case
            when arsm2 != floor(arsm2)                    -- has a decimal component
            and round(arsm2 * 1000) = arsm2 * 1000        -- and it's exactly 3 decimal digits
            then round(arsm2 * 1000)
            else arsm2
        end as arsm2
    FROM mapped
    WHERE arsm2 > 0
)

-- Add usdm2 column
SELECT
    CAST(c.date AS DATE) AS date,
    c.model_type,
    c.arsm2,
    c.arsm2 / m.value AS usdm2
FROM cleaned AS c
LEFT JOIN {{ source("argmacro", "usdblue")}} AS m
ON c.date = m.date
ORDER BY c.date