with airlines as (
    select * from {{ ref("dim_airlines")}}
),
flight_summary as (
    select * from {{ ref('stg_flight_summary_data')}}
),

croissance_all as (
    select
        flight_summary.airline_code as airline_code,
        airlines.airline_name as airline_name,
        YEAR(flight_summary.date) as year,
        AVG(flight_summary.ASM_domestic) as avg_asm_domestic
    from flight_summary
    join airlines on flight_summary.airline_code = airlines.airline_code
    group by flight_summary.airline_code, airlines.airline_name, YEAR(flight_summary.date)
),


croissance as (
    select
        croissance_all.airline_code as airline_code,
        croissance_all.airline_name as airline_name,
        croissance_all.year as year, 
        croissance_all.avg_asm_domestic as avg_asm_domestic,
        lag(avg_asm_domestic , 1) over(partition by airline_code order by year asc) as previous_year,
        round(((avg_asm_domestic-previous_year)/(previous_year)*100),2) as taux_croissance
    from croissance_all
    order by airline_code, year
),

best_croissance as (
    select
        croissance.airline_code as airline_code,
        croissance.airline_name as airline_name,
        croissance.year as best_year, 
        croissance.taux_croissance as best_taux_croissance
    from croissance
    where croissance.taux_croissance is not NULL 
    QUALIFY ROW_NUMBER() OVER(
        partition by airline_code
        order by taux_croissance DESC 
    ) = 1
)

select * from best_croissance
