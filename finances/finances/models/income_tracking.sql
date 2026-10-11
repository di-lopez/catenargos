{% set classes = dbt_utils.get_column_values(
    table=ref("portfolioclass"),
    column="class",
    where="class is not null",
    order_by="min(class)"
) %}

WITH mapped AS (

    SELECT
        p.date,
        p.value,
        p.platform,
        m.class
    FROM {{ ref("portfolio_value") }} AS p
    LEFT JOIN {{ ref("portfolioclass") }} AS m
        ON p.platform = m.platform

),

grouped AS (

    SELECT
        date,
        class,
        SUM(value) AS value
    FROM mapped
    GROUP BY date, class

),

pivoted AS (

    SELECT *
    FROM grouped
    PIVOT (
        SUM(value)
        FOR class IN (
            {% for class_name in classes %}
                '{{ class_name | replace("'", "''") }}'
                    AS {{ adapter.quote(class_name) }}
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )

),

unioned AS (

SELECT *
FROM pivoted
UNION ALL
SELECT *
FROM {{ ref("historical_tracking") }}

)

with_totals AS (

    SELECT
        *,
        {% for class_name in classes %}
            COALESCE({{ adapter.quote(class_name) }}, 0)
            {% if not loop.last %} + {% endif %}
        {% endfor %} AS total
    FROM pivoted

)

SELECT
    *,
    total - LAG(total) OVER (ORDER BY date) AS delta
FROM with_totals
ORDER BY date