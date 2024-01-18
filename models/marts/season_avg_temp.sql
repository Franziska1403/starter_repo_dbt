/*looking for the difference between max and min temperature */
with total_avg as (
    select 
        date,
        city, 
        maxtemp_c, 
        mintemp_c,
        avgtemp_c,
        (maxtemp_c - mintemp_c) AS temperature_difference
    from {{(ref('prep_temp'))}}
    group by city, date, maxtemp_c, mintemp_c, avgtemp_c
),
/* season type here */
season_type as (
    select 
        date, 
        city,
        case
            when month_num in ('12', '01', '02') then 'winter'
            when month_num in ('03','04','05') then 'spring'
            when month_num in ('06','07','08') then 'summer'
            ELSE 'fall'
    end as season
    from {{(ref('prep_temp'))}}
    group by city, date, maxtemp_c, mintemp_c, avgtemp_c
), 
temp_evaluation as (
    SELECT
        avgtemp_c,
        case 
            when avgtemp_c < 0 then 'freezing'
            when avgtemp_c between 0.001 and 5 then 'very cold'
            when avgtemp_c between 5.1 and 12 then 'cold'
            when avgtemp_c between 12.1 and 17 then 'a little cold'
            when avgtemp_c between 17.1 and 22 then 'warm'
            when avgtemp_c between 22.1 and 30 then 'very warm'
            when avgtemp_c between 30.1 and 35 then 'hot'
            when avgtemp_c between 30.1 and 35 then 'very hot'
            when avgtemp_c > 35 then 'too hot'
            else 'not able to measure'
        end as temperature_feeling
    from {{(ref('prep_temp'))}}
    group by city, date, maxtemp_c, mintemp_c, avgtemp_c
)
select *
from total_avg
left join season_type using (date, city)
left join temp_evaluation using (avgtemp_c)