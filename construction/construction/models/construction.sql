WITH joined AS (
SELECT
    date,
    trim(lower(model_type)) as model_type,
    arsm2
FROM {{ source("silver", "cpau") }}
UNION ALL
SELECT
    date,
    trim(lower(replace(model_type, "_", " "))) as model_type,
    arsm2
FROM {{ ref("cpau_backdata") }}
)

clean_mapped AS (
    SELECT
        j.date,
        m.model_type,
        j.arsm2
    FROM joined AS j
    LEFT JOIN {{ ref("model_type_mapping") }} AS m
    ON j.model_type = m.raw_value
)

-- Clean incorrectly parsed arsm2 that treat thousands separator as decimal point
SELECT
    date,
    model_type,
    case
        when arsm2 != floor(arsm2)                    -- has a decimal component
        and round(arsm2 * 1000) = arsm2 * 1000        -- and it's exactly 3 decimal digits
        then round(arsm2 * 1000)
        else arsm2
    end as arsm2
FROM clean_mapped