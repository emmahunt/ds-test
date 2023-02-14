select distinct
    race_id
    , horse_count
    , course_distance_metres
    , meeting_date
    , meeting_id
from {{ ref('stg_horses') }}

