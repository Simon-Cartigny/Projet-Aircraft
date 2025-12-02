with airports as (
    select * from {{ ref('dim_airports')}}
),
aircrafts as (
    select * from {{ ref('dim_aircrafts')}}
),
individual_flights as (
    select * from {{ ref('stg_individual_flights')}}
),

flights_capacity as (
    select 
        individual_flights.flight_id as flight_id,
        individual_flights.aircraft_id as individual_flights,
        aircrafts.capacity as flight_capacity,
        individual_flights.departure_airport_code as departure_airport_code,
        individual_flights.destination_airport_code as destination_airport_code
    from individual_flights
    join aircrafts on individual_flights.aircraft_id = aircrafts.aircraft_id
),

capacity as (
    select 
        flights_capacity.departure_airport_code as airport_code,
        flights_capacity.flight_capacity as airport_capacity
    from flights_capacity
UNION ALL
    select 
        flights_capacity.destination_airport_code as airport_code, 
        flights_capacity.flight_capacity as airport_capacity
    from flights_capacity
),

airport_capacity as (
    select 
        capacity.airport_code as airport_code,
        sum(capacity.airport_capacity) as airport_capacity
    from capacity
    group by capacity.airport_code
)
select * from airport_capacity