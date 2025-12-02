with flight_summary as (
    select
        "index",
        to_date("Date", 'DD/MM/YYYY') as date,
        coalesce("ASM_Domestic", 0) as ASM_Domestic,
        coalesce("ASM_International", 0) as ASM_international,
        coalesce("Flights_Domestic", 0) as flights_domestic,
        coalesce("Flights_International", 0) as flights_international,
        coalesce("Passengers_Domestic", 0) as passengers_domestic,
        coalesce("Passengers_International", 0) as passengers_international,
        coalesce("RPM_Domestic", 0) as RPM_domestic,
        coalesce("RPM_International", 0) as RPM_international,
        "Airline_Code" as airline_code,
        "Airport_Code" as airport_code
    from {{source('aircraft_analytics', 'flight_summary_data')}}
)
select * from flight_summary