with horses as (
    
    -- Clean up the forecast price and starting price columns, which excel erroneously converts to dates
    select 
        *
    , case
        when contains(forecastprice, '-') 
            then concat(
                get(split(forecastprice, '-'), 0)
                , '/'
                , month(try_to_date(forecastprice, 'dd-mon'))
            )
        else forecastprice
    end as forecast_price_fraction
    , case
        when contains(startingprice, '-') 
            then concat(
                get(split(startingprice, '-'), 0)
                , '/'
                , month(try_to_date(startingprice, 'dd-mon'))
            )
        else startingprice
    end as starting_price_fraction

    from {{ ref('horses') }}
)

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
    , forecast_price_fraction
    
    -- Format the forecast price as a percentage by calculating it from the fractional odds
    , get(split(forecast_price_fraction, '/'), 0) / get(split(forecast_price_fraction, '/'), 1) as forecast_price_decimal
    , statcourse as stat_course
    , statdistance as stat_distance

    , starting_price_fraction
    -- Format the starting price as a percentage by calculating it from the fractional odds
    , get(split(starting_price_fraction, '/'), 0) / get(split(starting_price_fraction, '/'), 1) as starting_price_percentage
    
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
    , course_distance as course_distance_metres

    -- A try cast is not used here, as a "loud" failure in the cast will alert that there is new data in an unexpected format
    , to_date(meetingdate, 'DD/MM/YYYY') as meeting_date
    , cast(won as boolean) as won
from horses

