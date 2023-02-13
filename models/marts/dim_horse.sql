select
    horse_id
    -- , weight_value
    -- , trainer_id
    -- , last_run_days_flat
    , gender
    , case
        when gender in ('colt', 'stallion', 'gelding') then 'male'
        when gender in ('filly', 'mare', 'broodmare', 'dam') then 'female'
    end as sex
    , colour
    , year_born
    , count_if(won) as number_of_races_won
from {{ ref('stg_horses') }}
group by 1, 2, 3, 4, 5
