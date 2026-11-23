with transactions as (

    select *
    from {{ source('raw', 'balance_statements') }}

),

seed_rule_matches as (

    select
        t.*,
        r.rule_id,
        r.reason
    from transactions as t
    inner join {{ ref('transaction_filter_rules') }} as r
        on r.enabled = true
       and (
            (
                r.rule_type = 'transaction_id'
                and t.transaction_id = r.pattern
            )
            or
            (
                r.rule_type = 'description_regex'
                and regexp_like(coalesce(t.description, ''), r.pattern)
            )
       )

),

seed_filtered as (

    select t.*
    from transactions as t
    left anti join {{ ref('transaction_filter_rules') }} as r
        on r.enabled = true
       and (
            (
                r.rule_type = 'transaction_id'
                and t.transaction_id = r.pattern
            )
            or
            (
                r.rule_type = 'description_regex'
                and regexp_like(coalesce(t.description, ''), r.pattern)
            )
       )

),

accounting_matches as (

    select distinct
        x.*,
        'R000' as rule_id,
        'Accounting: equal and opposite transaction' as reason
    from seed_filtered as x
    inner join seed_filtered as y
        on x.date = y.date
       and x.amount = -1 * y.amount
       and x.transaction_id <> y.transaction_id

),

all_matches as (

    select * from seed_rule_matches
    union all
    select * from accounting_matches

),

ranked_matches as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by rule_id
        ) as rule_priority
    from all_matches

)

select * except (rule_priority)
from ranked_matches
where rule_priority = 1