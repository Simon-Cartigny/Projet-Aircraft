with airlines as (
    select * from {{ref('dim_airlines')}}
),
flight_summary as (
    select * from {{ref('stg_flight_summary_data')}}
),

-- creation cte pour calculer total par année et par compagnie
airlines_summary as (
    select
        YEAR(flight_summary.date) as year,
        flight_summary.airline_code,
        airlines.airline_name,
        sum(flight_summary.RPM_domestic) as domestic_revenue,
        sum(flight_summary.RPM_international) as international_revenue,
        (sum(flight_summary.RPM_domestic) +
sum(flight_summary.RPM_international)) as total_revenue
    FROM flight_summary
    JOIN airlines ON flight_summary.airline_code = airlines.airline_code
    GROUP BY 
        YEAR(flight_summary.date),
        flight_summary.airline_code,
        airlines.airline_name
)

select * FROM airlines_summary