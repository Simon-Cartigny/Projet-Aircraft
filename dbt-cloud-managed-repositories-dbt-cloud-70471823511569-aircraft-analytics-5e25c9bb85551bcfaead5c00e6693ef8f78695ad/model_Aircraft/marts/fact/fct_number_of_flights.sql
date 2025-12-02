{{ config (
    materialized="table"
)}}

WITH individual_flights_dim AS
(SELECT * FROM {{ ref('dim_individual_flights')}}),

aircraft_dim AS
(SELECT * FROM {{"dim_aircrafts"}})

SELECT
    aircraft_dim.aircraft_type,
    individual_flights_dim.aircraft_id,
    COUNT(individual_flights_dim.flight_id) as number_of_flights
FROM individual_flights_dim
JOIN aircraft_dim ON aircraft_dim.aircraft_id = individual_flights_dim.aircraft_id
GROUP BY individual_flights_dim.aircraft_id, aircraft_dim.aircraft_type
ORDER BY COUNT(*) DESC