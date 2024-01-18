/*looking for the difference between max and min temperature */
with total_avg as (
    select 
        date,
        city, 
        maxtemp_c, 
        mintemp_c,
        avgtemp_c
        MAX(maxtemp_c) - MIN(mintemp_c) AS temperature_difference
    from {{(ref('prep_temp'))}}
    group by city, date, year, maxtemp_c, mintemp_c,
),
/* season type here */
season_type as (
    select 
        date, 
        city
        case
            when month_num in ('12', '1', '2') then 'winter'
            when month_num in ('3','4','5') then 'spring'
            when month_num in ('6','7','8') then 'summer'
            ELSE 'fall'
    end as season
    from {{(ref('prep_temp'))}}
    group by city, season
), 
temp_evaluation as (
    SELECT
        avgtemp_c 
        case 
            when avgtemp_c is < 0 then 'freezing'
            when avgtemp_c is between 0.001 and 5 then 'very cold'
            when avgtemp_c is between 5.1 and 12 then 'cold'
            when avgtemp_c is between 12.1 and 17 then 'a little cold'
            when avgtemp_c is between 17.1 and 22 then 'warm'
            when avgtemp_c is between 22.1 and 30 then 'very warm'
            when avgtemp_c is between 30.1 and 35 then 'hot'
            when avgtemp_c is between 30.1 and 35 then 'very hot'
            when avgtemp_c is > 35 then 'too hot'
            else 'not able to measure'
        end as temperature_feeling
    from {{(ref('prep_temp'))}}
), 
select *
from prep_temp 
left join season_type using (date, city)
left join temp_evaluation using (avgtemp_c);



