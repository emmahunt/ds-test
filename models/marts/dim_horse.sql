
with horses as (
    select *
        , min(meeting_date) over (partition by horse_id order by meeting_date asc) as first_meeting_date
        , lag(trainer_id) over (partition by horse_id order by meeting_date asc) as previous_trainer_id
    from {{ ref('stg_horses') }}
)

, identify_trainer_change as (
    select distinct
        horse_id
        , gender
        , colour
        , year_born
        , first_meeting_date
        , trainer_id
        , meeting_date
    from horses
    where 
        meeting_date = first_meeting_date 
            or trainer_id != previous_trainer_id
)

, final as (
    select
        horse_id
        , gender
        , case
            when gender in ('colt', 'stallion', 'gelding') then 'male'
            when gender in ('filly', 'mare', 'broodmare', 'dam') then 'female'
        end as sex
        , colour
        , year_born
        , first_meeting_date
        , trainer_id
        , meeting_date as valid_from
        , lead(meeting_date) over (partition by horse_id order by meeting_date asc) as valid_to
    from identify_trainer_change
)

select * from final
