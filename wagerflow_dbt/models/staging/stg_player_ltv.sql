with source as (
    select * from {{ source('analytics', 'player_ltv') }}
)

select 
    player_id,
    total_stake,
    total_payout,
    ltv,
    bet_count
from source