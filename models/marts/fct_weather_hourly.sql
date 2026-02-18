{{ config(
    materialized='incremental',
    unique_key=['location', 'time_hour'],
    incremental_strategy='merge'
) }}

with base as (
    select
        location,
        time_hour,
        temperature_c,
        temperature_f,
        precipitation_probability,
        precipitation_mm,
        is_precipitating,
        is_day
    from {{ ref('stg_weather') }}
    {% if is_incremental() %}
      {% set cols = adapter.get_columns_in_relation(this) %}
      {% set colnames = cols | map(attribute='name') | list %}
      {% if 'TIME_HOUR' in colnames %}
        where time_hour > (select coalesce(max(time_hour), '1900-01-01'::timestamp_ntz) from {{ this }})
      {% else %}
        -- Backward-compat: existing table has TIME column from earlier version
        where time_hour > (select coalesce(max(time), '1900-01-01'::timestamp_ntz) from {{ this }})
      {% endif %}
    {% endif %}
),
final as (
    select
        location,
        time_hour,
        cast(time_hour as date) as date,
        extract(hour from time_hour) as hour_of_day,
        dayofweekiso(time_hour) as weekday_iso,
        dayname(time_hour) as weekday_name,
        month(time_hour) as month_num,
        monthname(time_hour) as month_name,
        iff(dayofweekiso(time_hour) in (6, 7), true, false) as is_weekend,
        temperature_c,
        temperature_f,
        precipitation_probability,
        precipitation_mm,
        is_precipitating,
        is_day,
        iff(is_day, 'day', 'night') as day_night,
        case
            when precipitation_mm = 0 then 'none'
            when precipitation_mm <= 2.5 then 'light'
            when precipitation_mm <= 10 then 'moderate'
            else 'heavy'
        end as precipitation_intensity,
        floor(coalesce(precipitation_probability, 0) / 20) * 20 as precipitation_prob_bucket_start
    from base
)
select * from final
