with src as (
    select
        cast(location as varchar) as location_raw,
        cast(time as timestamp_ntz) as time_raw,
        cast(temperature as float) as temperature_c_raw,
        cast(precipitation_probability as float) as precipitation_probability_raw,
        cast(precipitation as float) as precipitation_raw,
        cast(is_day as number) as is_day_num_raw
    from {{ source('raw', 'WEATHER') }}
),
clean as (
    select
        upper(trim(location_raw)) as location,
        -- Truncate to the hour for a consistent hourly grain
        cast(date_trunc('hour', time_raw) as timestamp_ntz) as time_hour,
        temperature_c_raw as temperature_c,
        -- Convert C to F for convenience
        (temperature_c_raw * 9/5) + 32 as temperature_f,
        -- Clamp probability to [0,100]; default missing to 0
        least(greatest(coalesce(precipitation_probability_raw, 0), 0), 100) as precipitation_probability,
        -- No negative precipitation; default missing to 0
        greatest(coalesce(precipitation_raw, 0), 0) as precipitation_mm,
        -- Boolean flags
        case when greatest(coalesce(precipitation_raw, 0), 0) > 0 then true else false end as is_precipitating,
        case when is_day_num_raw = 1 then true when is_day_num_raw = 0 then false else null end as is_day
    from src
)
select
    location,
    time_hour,
    temperature_c,
    temperature_f,
    precipitation_probability,
    precipitation_mm,
    is_precipitating,
    is_day
from clean
