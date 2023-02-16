select 
    race_id
    , horse_id
    , cloth
    , stall
    , weight_value
    , last_run_days_flat
    , cheek_pieces
    , forecast_price_fraction
    , forecast_price_decimal
    , stat_course
    , stat_distance
    , starting_price_fraction
    , starting_price_decimal
    , jockey_id
    , trainer_id
from {{ ref('stg_horses') }}