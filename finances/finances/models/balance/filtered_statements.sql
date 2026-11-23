with transactions as (

    select *
    from {{ source('raw', 'balance_statements') }}

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

-- Transactions between own accounts, card payments, etc
accounting_matches as (

    select distinct x.transaction_id
    from seed_filtered as x
    inner join seed_filtered as y
        on x.date = y.date
       and x.amount = -1 * y.amount
       and x.transaction_id <> y.transaction_id

)

select t.*
from seed_filtered as t
left anti join accounting_matches as a
    on t.transaction_id = a.transaction_id