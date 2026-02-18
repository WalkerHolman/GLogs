{{ config({"severity":"Warn"}) }}
{{ test_unique(column_name="time_hour", model=get_where_subquery(ref('fct_weather_hourly'))) }}