WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_temp')}}
),
add_weekday AS (
    SELECT *,
        to_chart (date, 'day') AS weekday,
        to_chart (date, 'DD') AS day_num
    FROM temp_daily
)
SELECT *
FROM add_weekday

