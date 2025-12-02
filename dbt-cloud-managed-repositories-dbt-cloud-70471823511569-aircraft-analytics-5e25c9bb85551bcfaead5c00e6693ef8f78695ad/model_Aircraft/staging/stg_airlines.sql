with
    airlines as (
        select
            "Airline_Code" as airline_code,
            "Airline_Name" as airline_name,
            "Market_Cap" as market_cap,
            "Employees" as employees,
            "Age" as age

        from {{ source("aircraft_analytics", "airlines") }}
    )
select *
from airlines
