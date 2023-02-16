
with stg_horses as (select * from {{ ref('stg_horses') }})

, winners as (
    select distinct 
        race_id
        , horse_id
    from stg_horses
    where won
)

, final as (
    select
        stg_horses.race_id
        , stg_horses.course_distance_metres
        , stg_horses.meeting_date
        , stg_horses.horse_count
        , stg_horses.meeting_id
        , stg_horses.weather
        , array_agg(distinct winners.horse_id) as winning_horse_ids 
        , count(distinct stg_horses.horse_id) as number_of_horses_in_race
    from stg_horses
    left join winners
        on stg_horses.race_id = winners.race_id
    group by 1, 2, 3, 4, 5
)

select * from final
