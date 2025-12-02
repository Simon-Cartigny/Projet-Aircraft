with airports as (
    select * from {{ref('dim_airports')}}
),
flight_summary as (
    select * from {{ref('stg_flight_summary_data')}}
),
airports_summary as (
    select
        airports.airport_code,
        airports.airport_name,
        sum(flight_summary.passengers_domestic) +
sum(flight_summary.passengers_international) as passengers_nb
    from airports
    join flight_summary on airports.airport_code = flight_summary.airport_code
    group by 
        airports.airport_code,
        airports.airport_name
)
select * from airports_summary