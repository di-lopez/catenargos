with fx as (
    select
        t.transaction_id,
        case
            when currency = 'gbp' then 1
            when currency = 'usd' then 1/1.3 -- usd to gbp
            when currency = 'ars' then 1/(blue.value * 1.3) -- ars to gbp
        end as fx
    from {{ref("classified_statements") }}as t
    left join {{source("argmacro", "usdblue")}} as blue
        on t.date = blue.date
)

select
    date,
    description,
    amount * fx as amount,
    category,
    source,
    transaction_id
from {{ref('classified_statements')}}
left join fx
using(transaction_id)
order by date





