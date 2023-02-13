select
    raceid  as race_id
    , horseid as horse_id
    , cloth
    , stall
    , weightvalue as weight_value
    , jockeyid as jockey_id
    , trainerid as trainer_id
    , lastrundaysflat as last_run_days_flat
    , age
    , cheekpieces as cheek_pieces
    , case
        when contains(forecastprice, '/') then forecastprice
        else null
    end as forecast_price_fraction
    
    -- Format the forecast price as a percentage by calculating it from the fractional odds
    , case
        when contains(forecastprice, '/') then 100 / (forecastprice + 1) * 100
        else null
    end as forecast_price_percentage
    , try_to_date(forecastprice, 'dd-mon') as forecast_date
    , statcourse as stat_course
    , statdistance as stat_distance
    -- Format the starting price as a percentage by calculating it from the fractional odds
    , case
        when contains(startingprice, '/') then 100 / (startingprice + 1) * 100
        else null
    end as starting_price_percentage
    , try_to_date(startingprice, 'dd-mon') as starting_date
    
    -- Reformat the horse gender as in line with definitions found here
    -- https://www.equineworld.co.uk/about-horses/horse-gender-definitions
    -- there are two possible sexes for values of 'f': foal or filly
    -- Assuming 'filly' as a foal is likely too young to be in a horse race
    
    , case
        when sex = 'g' then 'gelding'
        when sex = 'f' then 'filly'
        when sex = 'm' then 'mare'
        when sex = 'c' then 'colt'

        -- unable to find a definition for this gender
        when sex = 'h' then 'h'
        else sex
    end as gender
    , case
        when colour = 'b' then 'black'
        when colour = 'ch' then 'chestnut'
        when colour = 'gr' then 'gray'
        when colour = 'br' then 'brown'
        when colour = 'b/b' then 'black'
        when colour = 'dkb' then 'dark brown'
        when colour = 'bl/' then 'black'
    end as colour
    , yearborn as year_born
    , meetingid as meeting_id
    , scheduledtime as scheduled_time
    , horsecount as horse_count

    -- Create a string format comma separated list of weather descriptions
    -- so that in future they can be isolated / independently applicable
    -- e.g. 'Sunny' and 'Windy' applied to one day, instead of the string 'Sunny & Windy'
    , case
        when weather = 'Fine but Cloudy' then 'Fine,Cloudy' 
        else replace(weather, ' & ', ',')
    end as weather
    , course_distance

    -- A try cast is not used here, as a "loud" failure in the cast will alert that there is new data in an unexpected format
    , to_date(meetingdate, 'DD/MM/YYYY') as meeting_date
    , cast(won as boolean) as won
from {{ ref('horses') }}

