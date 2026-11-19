{% set classes = dbt_utils.get_column_values(
    table=ref("portfolioclass"),
    column="class",
    where="class is not null",
    order_by="min(class)"
) %}

{% set cols = [] %}
{% set pivot_values = [] %}
{% set total_terms = [] %}
{% for c in classes %}
    {% do cols.append(adapter.quote(c)) %}
    {% do pivot_values.append("'" ~ c | replace("'", "''") ~ "' AS " ~ adapter.quote(c)) %}
    {% do total_terms.append("COALESCE(" ~ adapter.quote(c) ~ ", 0)") %}
{% endfor %}

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
    SELECT date, {{ cols | join(", ") }}
    FROM grouped
    PIVOT (
        SUM(value)
        FOR class IN ({{ pivot_values | join(", ") }})
    )
),

unioned AS (
    SELECT date, {{ cols | join(", ") }} FROM pivoted
    UNION ALL
    SELECT date, {{ cols | join(", ") }} FROM {{ ref("historical_tracking") }}
),

with_totals AS (
    SELECT
        date,
        {{ cols | join(", ") }},
        {{ total_terms | join(" + ") }} AS total
    FROM unioned
)

SELECT
    date,
    {{ cols | join(", ") }},
    total,
    total - LAG(total) OVER (ORDER BY date) AS delta
FROM with_totals
ORDER BY date