
with stg_horses as (select * from {{ ref('stg_horses') }})

, winners as (
    select race_id
        , array_agg(distinct horse_id) as winning_horse_ids
    from stg_horses
    where won
    group by 1
)

, final as (
    select distinct
        stg_horses.race_id
        , winners.winning_horse_ids
        , stg_horses.horse_count
        , stg_horses.course_distance_metres
        , stg_horses.meeting_date
        , stg_horses.meeting_id
    from stg_horses
    left join winners
        on stg_horses.race_id = winners.race_id
)

select * from final
