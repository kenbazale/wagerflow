with source as (
    select * from {{ source('analytics', 'ggr_daily') }}
)

select 
    settlement_date,
    market_id,
    total_stake,
    total_payout,
    ggr,
    bet_count
from source