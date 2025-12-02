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

-- classement par fenetre pour afficher top1 domestic, top1 international, top1 total par compagnie

SELECT
    airlines_summary.airline_code,
    airlines_summary.airline_name,
    year,
    domestic_revenue as total_revenue,
    'Revenu Domestique' as metrique
FROM airlines_summary
QUALIFY ROW_NUMBER() OVER(
    partition by airline_code
    order by domestic_revenue DESC
) = 1
UNION ALL 
SELECT
    airlines_summary.airline_code,
    airlines_summary.airline_name,
    year,
    international_revenue as total_revenue,
    'Revenu International' as metrique
FROM airlines_summary
QUALIFY ROW_NUMBER() OVER(
    partition by airline_code
    order by international_revenue DESC 
) = 1
UNION ALL 
SELECT 
    airlines_summary.airline_code,
    airlines_summary.airline_name,
    year,
    total_revenue as total_revenue,
    'Revenu Total' as metrique
FROM airlines_summary
QUALIFY ROW_NUMBER() OVER(
    partition by airline_code
    order by total_revenue DESC
) = 1