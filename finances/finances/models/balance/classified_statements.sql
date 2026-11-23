with transactions as (

    select *
    from {{ ref('filtered_statements') }}

),

matched_categories as (

    select
        t.transaction_id,
        lower(r.category) as category,
        row_number() over (
            partition by t.transaction_id
            order by r.rule_id
        ) as match_rank
    from transactions as t
    inner join {{ ref('categories') }} as r
        on regexp_like(coalesce(t.description, ''), r.pattern)

),

first_match as (

    select
        transaction_id,
        category
    from matched_categories
    where match_rank = 1

)

select
    t.*,
    coalesce(c.category, 'other') as category
from transactions as t
left join first_match as c
    on t.transaction_id = c.transaction_id